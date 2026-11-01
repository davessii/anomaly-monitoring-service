"""Real local file operations; clocks are explicit, no live app or model."""
from dataclasses import FrozenInstanceError
from pathlib import Path
import os
import tempfile
import unittest
from unittest.mock import patch

from .log_aggregation import LogAggregator
from .log_parser import LogParseError, parse_line
from .log_schema import MAX_LINE_BYTES
from .log_tailer import (LogTailer, TailerError, READ_CHUNK_BYTES,
                         MAX_EVENTS_PER_POLL, SAMPLE_BYTES)

BASE = 1792490400000


def line(offset=100, status=200, duration=40):
    stamp = BASE+offset
    return (f'v1 ts={stamp//1000}.{stamp%1000:03d} status={status} '
            f'rt={duration//1000}.{duration%1000:03d}\n').encode('ascii')


class LogTailerTests(unittest.TestCase):
    def setUp(self):
        self.temp_root = Path(__file__).resolve().parents[1]/'work/log-tailer-test-tmp'
        self.temp_root.mkdir(exist_ok=True)
        self.directory = tempfile.TemporaryDirectory(prefix='case-', dir=self.temp_root)
        self.addCleanup(self.cleanup_directory)
        self.path = Path(self.directory.name)/'access.log'
        self.path.write_bytes(b'')

    def cleanup_directory(self):
        target, root = Path(self.directory.name).resolve(), self.temp_root.resolve()
        # Verify containment before TemporaryDirectory recursively removes its files.
        if target == root or not target.is_relative_to(root):
            raise RuntimeError('Test cleanup escaped its workspace directory')
        self.directory.cleanup()

    def append(self, data):
        with self.path.open('ab', buffering=0) as stream:
            stream.write(data)

    def engine(self, **kwargs):
        return LogTailer(self.path, source_id='test-app', **kwargs)

    def assert_stops(self, engine, reason, now=BASE+1000):
        before = engine.diagnostics()['read_offset']
        with self.assertRaises(TailerError) as caught:
            engine.poll(now)
        self.assertEqual(caught.exception.code, reason)
        self.assertEqual((engine.status, engine.reason), ('needs_restart', reason))
        self.assertEqual(engine.diagnostics()['read_offset'], before)
        with self.assertRaises(TailerError) as again:
            engine.poll(now)
        self.assertEqual(again.exception.code, 'source_not_active')

    def test_live_starts_at_eof_then_reads_only_new_complete_lines(self):
        old = line(10)
        self.path.write_bytes(old)
        engine = self.engine()
        self.assertEqual(engine.poll(BASE+100).events, ())
        new = line(200, 503)
        self.append(new)
        batch = engine.poll(BASE+250)
        event, = batch.events
        self.assertEqual((event.data, event.start_offset, event.end_offset),
                         (new, len(old), len(old)+len(new)))
        self.assertEqual(event.read_at_ms, BASE+250)
        self.assertEqual(engine.poll(BASE+300).events, ())

    def test_replay_is_explicit_and_does_not_claim_live_availability(self):
        self.path.write_bytes(line(100)+line(200))
        engine = self.engine(mode='replay')
        batch = engine.poll(BASE+500)
        self.assertEqual(len(batch.events), 2)
        self.assertTrue(all(event.read_at_ms == BASE+500 for event in batch.events))
        self.assertEqual(engine.diagnostics()['mode'], 'replay')

    def test_live_availability_is_sampled_after_bytes_and_checks(self):
        engine = self.engine()
        self.append(line(101))  # Writer appended after the cycle sampled time 100.
        split = engine._split
        read_finished = []
        def record_read(data, now_ms):
            read_finished.append(data)
            return split(data, now_ms)
        def clock():
            self.assertEqual(read_finished, [line(101)])
            return BASE+102
        with patch.object(engine, '_split', side_effect=record_read):
            batch = engine.poll(BASE+100, read_clock_ms=clock)
        self.assertEqual(batch.read_completed_at_ms, BASE+102)
        self.assertEqual(batch.events[0].read_at_ms, BASE+102)
        self.assertEqual(batch.events[0].data, line(101))
        self.assertEqual(engine.diagnostics()['last_clock_ms'], BASE+102)
        aggregate = LogAggregator(BASE)
        self.assertEqual(aggregate.add(parse_line(batch.events[0].data),
                                      read_at_ms=batch.events[0].read_at_ms), 'accepted')

    def test_post_read_clock_backwards_does_not_commit_offsets(self):
        engine = self.engine()
        self.append(line(100))
        with self.assertRaises(TailerError) as caught:
            engine.poll(BASE+101, read_clock_ms=lambda: BASE+100)
        self.assertEqual(caught.exception.code, 'clock_went_backwards')
        self.assertEqual(engine.diagnostics()['read_offset'], 0)
        self.assertEqual(engine.diagnostics()['total_events'], 0)
        self.assertEqual(engine.status, 'needs_restart')

    def test_empty_read_still_records_completion_clock(self):
        engine = self.engine()
        batch = engine.poll(BASE+100, read_clock_ms=lambda: BASE+102)
        self.assertEqual(batch.events, ())
        self.assertEqual(batch.read_completed_at_ms, BASE+102)
        self.assertEqual(engine.diagnostics()['last_clock_ms'], BASE+102)

    def test_split_line_uses_clock_when_final_bytes_are_read(self):
        engine = self.engine()
        value = line(101)
        self.append(value[:10])
        engine.poll(BASE+100, read_clock_ms=lambda: BASE+102)
        self.append(value[10:])
        batch = engine.poll(BASE+103, read_clock_ms=lambda: BASE+105)
        self.assertEqual(batch.events[0].data, value)
        self.assertEqual(batch.events[0].read_at_ms, BASE+105)

    def test_partial_line_waits_without_emitting_or_advancing_complete_offset(self):
        engine = self.engine()
        value = line()
        self.append(value[:17])
        first = engine.poll(BASE+100)
        self.assertEqual(first.events, ())
        self.assertEqual(first.last_complete_offset, 0)
        self.assertTrue(first.waiting_for_lf)
        self.append(value[17:])
        event, = engine.poll(BASE+150).events
        self.assertEqual((event.data, event.start_offset, event.end_offset), (value, 0, len(value)))
        self.assertFalse(engine.diagnostics()['waiting_for_lf'])

    def test_live_discards_remainder_of_preexisting_partial_line(self):
        self.path.write_bytes(b'old unfinished')
        engine = self.engine()
        self.assertIsNone(engine.diagnostics()['last_complete_offset'])
        self.append(b' remainder\n'+line(200))
        skipped, event = engine.poll(BASE+250).events
        self.assertEqual(skipped.kind, 'skipped_startup_tail')
        self.assertIsNone(skipped.data)
        self.assertEqual(skipped.start_offset, len(b'old unfinished'))
        self.assertEqual(event.kind, 'line')
        self.assertEqual(event.data, line(200))

    def test_live_old_cr_waits_for_lf_and_does_not_create_blank_new_request(self):
        self.path.write_bytes(b'old\r')
        engine = self.engine()
        self.append(b'\n'+line(200))
        skipped, request = engine.poll(BASE+250).events
        self.assertEqual(skipped.kind, 'skipped_startup_tail')
        self.assertEqual(request.data, line(200))

    def test_replay_retains_partial_historical_line_until_finished(self):
        value = line()
        self.path.write_bytes(value[:-1])
        engine = self.engine(mode='replay')
        self.assertEqual(engine.poll(BASE+100).events, ())
        self.append(b'\n')
        self.assertEqual(engine.poll(BASE+150).events[0].data, value)

    def test_crlf_crossing_single_byte_reads_remains_unchanged(self):
        value = line()[:-1]+b'\r\n'
        self.path.write_bytes(value)
        engine = self.engine(mode='replay', read_chunk_bytes=1)
        events = []
        for _ in range(len(value)):
            events.extend(engine.poll(BASE+200).events)
        self.assertEqual([event.data for event in events], [value])
        self.assertEqual(parse_line(events[0].data).request_time_ms, 40)

    def test_utf8_is_not_decoded_until_full_line_and_rejected_by_parser(self):
        engine = self.engine(read_chunk_bytes=1)
        value = 'ошибка\n'.encode('utf-8')
        self.append(value)
        events = []
        for _ in range(len(value)):
            events.extend(engine.poll(BASE+200).events)
        self.assertEqual([event.data for event in events], [value])
        with self.assertRaises(LogParseError) as caught:
            parse_line(events[0].data)
        self.assertEqual(caught.exception.code, 'format_mismatch')

    def test_identical_requests_have_distinct_byte_event_ids(self):
        value = line()
        self.path.write_bytes(value*2)
        engine = self.engine(mode='replay', generation='generation-1')
        first, second = engine.poll(BASE+200).events
        self.assertEqual(first.data, second.data)
        self.assertNotEqual(first.event_id, second.event_id)
        self.assertEqual(first.event_id, ('test-app', 'generation-1', 0))
        self.assertEqual(second.event_id[-1], len(value))

    def test_events_and_batches_are_immutable(self):
        self.path.write_bytes(line())
        batch = self.engine(mode='replay').poll(BASE+200)
        with self.assertRaises(FrozenInstanceError):
            batch.events[0].end_offset = 99
        with self.assertRaises(FrozenInstanceError):
            batch.read_offset = 99

    def test_exact_line_limit_and_limit_plus_one(self):
        self.path.write_bytes(b'a'*(MAX_LINE_BYTES-1)+b'\n'+b'b'*MAX_LINE_BYTES+b'\n')
        engine = self.engine(mode='replay')
        allowed, oversized = engine.poll(BASE+200).events
        self.assertEqual((allowed.kind, len(allowed.data)), ('line', MAX_LINE_BYTES))
        self.assertEqual(oversized.kind, 'line_too_long')
        self.assertIsNone(oversized.data)
        self.assertEqual(len(oversized.sample), SAMPLE_BYTES)
        self.assertEqual(oversized.end_offset-oversized.start_offset, MAX_LINE_BYTES+1)

    def test_oversized_unfinished_line_is_discarded_with_bounded_memory(self):
        value_size = 2*1024*1024
        self.path.write_bytes(b'x'*value_size)
        engine = self.engine(mode='replay')
        for _ in range(value_size//READ_CHUNK_BYTES):
            batch = engine.poll(BASE+200)
            self.assertLessEqual(batch.bytes_read, READ_CHUNK_BYTES)
            self.assertEqual(batch.events, ())
            state = engine.diagnostics()
            self.assertEqual(state['buffered_bytes'], 0)
            self.assertLessEqual(state['sample_bytes'], SAMPLE_BYTES)
        self.assertEqual(engine.diagnostics()['discard_mode'], 'oversize')
        self.append(b'\n'+line(300))
        bad, good = engine.poll(BASE+400).events
        self.assertEqual(bad.end_offset-bad.start_offset, value_size+1)
        self.assertEqual((bad.kind, good.kind), ('line_too_long', 'line'))
        self.assertEqual(good.data, line(300))

    def test_line_limit_without_lf_is_known_oversize_but_report_waits_for_lf(self):
        self.path.write_bytes(b'x'*MAX_LINE_BYTES)
        engine = self.engine(mode='replay')
        self.assertEqual(engine.poll(BASE+100).events, ())
        self.assertEqual(engine.diagnostics()['discard_mode'], 'oversize')
        self.append(b'\n')
        self.assertEqual(engine.poll(BASE+200).events[0].kind, 'line_too_long')

    def test_each_poll_bounds_bytes_and_event_count_without_losing_remainder(self):
        self.path.write_bytes(b'\n'*2000)
        engine = self.engine(mode='replay', read_chunk_bytes=1024, max_events_per_poll=7)
        events = []
        while engine.diagnostics()['backlog_bytes']:
            batch = engine.poll(BASE+100)
            self.assertLessEqual(batch.bytes_read, 1024)
            self.assertLessEqual(len(batch.events), 7)
            events.extend(batch.events)
        self.assertEqual(len(events), 2000)
        self.assertEqual([event.start_offset for event in events], list(range(2000)))
        self.assertEqual(engine.diagnostics()['read_offset'], 2000)

    def test_actual_file_identity_is_stable_across_append_and_differs_for_replacement(self):
        engine = self.engine()
        self.assertNotEqual(engine.identity.inode, 0)
        self.append(line())
        engine.poll(BASE+200)
        info = self.path.stat()
        self.assertEqual((info.st_dev, info.st_ino), (engine.identity.device, engine.identity.inode))
        replacement = self.path.with_name('replacement.log')
        replacement.write_bytes(line(300))
        self.assertNotEqual(replacement.stat().st_ino, engine.identity.inode)
        os.replace(replacement, self.path)
        self.assert_stops(engine, 'file_replaced', BASE+400)

    def test_detected_truncate_stops_without_retrying_from_zero(self):
        self.path.write_bytes(line())
        engine = self.engine(mode='replay')
        engine.poll(BASE+200)
        self.path.write_bytes(b'')
        self.assert_stops(engine, 'file_shrank')

    def test_missing_file_stays_stopped_even_after_recreation(self):
        engine = self.engine()
        self.path.unlink()
        self.assert_stops(engine, 'file_missing')
        self.path.write_bytes(line())
        with self.assertRaises(TailerError):
            engine.poll(BASE+2000)

    def test_anchor_detects_rewrite_even_when_file_grows(self):
        self.path.write_bytes(line())
        engine = self.engine()
        with self.path.open('r+b', buffering=0) as stream:
            stream.write(b'X')
            stream.seek(0, 2)
            stream.write(line(200))
        self.assert_stops(engine, 'file_content_changed')

    def test_same_size_metadata_change_is_ambiguous_and_stops(self):
        self.path.write_bytes(line())
        engine = self.engine()
        info = self.path.stat()
        os.utime(self.path, ns=(info.st_atime_ns, info.st_mtime_ns+10_000_000))
        self.assert_stops(engine, 'ambiguous_file_change')

    def test_append_during_poll_is_read_on_next_poll_without_loss_or_duplication(self):
        first, second = line(100), line(200)
        self.path.write_bytes(first)
        engine = self.engine(mode='replay')
        split = engine._split

        def append_during_split(data, now_ms):
            result = split(data, now_ms)
            self.append(second)
            return result

        with patch.object(engine, '_split', side_effect=append_during_split):
            batch = engine.poll(BASE+300)
        self.assertEqual([event.data for event in batch.events], [first])
        self.assertEqual(batch.backlog_bytes, len(second))
        self.assertEqual([event.data for event in engine.poll(BASE+400).events], [second])
        self.assertEqual(engine.poll(BASE+500).events, ())

    def test_truncate_during_poll_does_not_commit_parsed_candidates(self):
        self.path.write_bytes(line())
        engine = self.engine(mode='replay')
        split = engine._split

        def truncate_during_split(data, now_ms):
            result = split(data, now_ms)
            self.path.write_bytes(b'')
            return result

        with patch.object(engine, '_split', side_effect=truncate_during_split):
            self.assert_stops(engine, 'file_shrank')
        state = engine.diagnostics()
        self.assertEqual((state['total_events'], state['total_bytes_consumed']), (0, 0))

    def test_backwards_clock_stops_before_consuming_new_bytes(self):
        engine = self.engine()
        engine.poll(BASE+200)
        self.append(line())
        self.assert_stops(engine, 'clock_went_backwards', BASE+199)

    def test_close_is_explicit_and_does_not_reopen_on_poll(self):
        engine = self.engine()
        engine.close()
        self.assertEqual(engine.status, 'closed')
        with self.assertRaises(TailerError) as caught:
            engine.poll(BASE+200)
        self.assertEqual(caught.exception.code, 'source_not_active')

    def test_configuration_and_missing_source_validation(self):
        for kwargs in ({'source_id': ''}, {'source_id': 'bad/path'}, {'mode': 'guess'},
                       {'generation': ''}, {'read_chunk_bytes': 0},
                       {'read_chunk_bytes': READ_CHUNK_BYTES+1},
                       {'max_events_per_poll': MAX_EVENTS_PER_POLL+1},
                       {'max_events_per_poll': True}):
            with self.subTest(kwargs=kwargs):
                options = dict(source_id='test-app')
                options.update(kwargs)
                with self.assertRaises(ValueError):
                    LogTailer(self.path, **options)
        self.path.unlink()
        with self.assertRaises(TailerError) as caught:
            self.engine()
        self.assertEqual(caught.exception.code, 'file_missing')

    def test_new_object_has_new_generation_and_no_implicit_resume(self):
        self.path.write_bytes(line())
        first = self.engine(mode='replay')
        first.poll(BASE+200)
        second = self.engine(mode='replay')
        self.assertNotEqual(first.generation, second.generation)
        self.assertEqual(second.diagnostics()['read_offset'], 0)
        self.assertFalse(second.diagnostics()['durable_checkpoint'])

    def test_tailer_parser_aggregator_known_values_no_duplicates(self):
        engine = self.engine()
        aggregate = LogAggregator(BASE)
        rows = [(100, 200, 40), (900, 503, 60), (1000, 200, 100),
                (1500, 404, 200), (1900, 500, 300), (2125, 200, 25)]
        for offset, status, duration in rows:
            self.append(line(offset, status, duration))
            batch = engine.poll(BASE+offset+50)
            for event in batch.events:
                self.assertEqual(event.kind, 'line')
                aggregate.add(parse_line(event.data), read_at_ms=event.read_at_ms)
            self.assertEqual(engine.poll(BASE+offset+50).events, ())
        windows = aggregate.advance(BASE+6000)
        self.assertEqual([w.request_count for w in windows], [2, 3, 1, 0])
        self.assertEqual([w.mean_request_ms for w in windows], [50, 200, 25, None])
        self.assertEqual([w.error_5xx_count for w in windows], [1, 1, 0, 0])
        self.assertEqual(aggregate.diagnostics()['accepted_records'], 6)

    def test_completed_bad_lines_mark_quality_partial_write_does_not(self):
        engine = self.engine()
        aggregate = LogAggregator(BASE)
        self.append(b'broken')
        self.assertEqual(engine.poll(BASE+100).events, ())
        self.assertFalse(aggregate.requires_restart)
        self.append(b'\n'+line(200))
        for event in engine.poll(BASE+300).events:
            try:
                record = parse_line(event.data)
            except LogParseError as exc:
                aggregate.note_parse_error(exc.code, read_at_ms=event.read_at_ms)
            else:
                aggregate.add(record, read_at_ms=event.read_at_ms)
        window, = aggregate.advance(BASE+3000)
        self.assertEqual(window.request_count, 1)
        self.assertFalse(window.eligible_for_model)
        self.assertEqual(window.quality, 'degraded')


if __name__ == '__main__':
    unittest.main()
