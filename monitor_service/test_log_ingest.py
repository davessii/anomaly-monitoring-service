"""Crash-boundary and restart software fixtures, NOT research experiments."""
from concurrent.futures import ThreadPoolExecutor
import copy
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from .store import Store, Conflict, encode
from .log_ingest import LogIngestor, LogIngestError, canonical, digest
from .log_checkpoint import pack, unpack
from .log_tailer import LogTailer
from .log_aggregation import LogAggregator
from .log_schema import FEATURES
from .immediate import MODE, ImmediateDetector
from .test_log_tailer import BASE, line


class LogIngestTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(__file__).resolve().parents[1]/'work/log-ingest-test-tmp'
        self.root.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix='case-', dir=self.root)
        self.addCleanup(self.cleanup_directory)
        self.path = Path(self.temp.name)/'access.log'
        self.path.write_bytes(b'')
        self.db_path = Path(self.temp.name)/'test.sqlite3'
        self.store = Store(self.db_path)
        self.store.register(dict(id='web', source='external', features=list(FEATURES),
                                 settings=dict(components=1, processing_mode=MODE)))
        self.ingest = LogIngestor(self.store)
        self.ingest.attach('access', 'web', self.path, start_ms=BASE)

    def cleanup_directory(self):
        target, root = Path(self.temp.name).resolve(), self.root.resolve()
        if target == root or not target.is_relative_to(root):
            raise RuntimeError('Test cleanup escaped workspace')
        self.temp.cleanup()

    def append(self, value):
        with self.path.open('ab', buffering=0) as stream:
            stream.write(value)

    def restart(self):
        self.store = Store(self.db_path)
        self.ingest = LogIngestor(self.store)

    def saved(self):
        with self.store.connection() as db:
            return {table: [tuple(row) for row in db.execute(f'SELECT * FROM {table} ORDER BY rowid')]
                    for table in ('hosts', 'observations', 'notifications', 'log_checkpoints', 'log_windows')}

    def prepare_window(self):
        self.append(line(100, 503, 60))
        self.ingest.poll('access', now_ms=BASE+150)
        self.ingest.poll('access', now_ms=BASE+3000)

    def test_cycle_read_race_accepts_request_and_restores_real_availability(self):
        self.append(line(101))
        self.ingest.poll('access', now_ms=BASE+100, read_clock_ms=lambda: BASE+102)
        self.restart()
        state = self.ingest.status('access')
        self.assertEqual(state['status'], 'active')
        self.assertEqual(state['aggregate']['accepted_records'], 1)
        self.assertEqual(state['reader']['last_clock_ms'], BASE+102)
        self.assertEqual(state['aggregate']['last_clock_ms'], BASE+102)
        self.ingest.poll('access', now_ms=BASE+2999, read_clock_ms=lambda: BASE+3001)
        window = self.ingest.windows('access')[0]['payload']
        self.assertEqual(window['finalized_at_ms'], BASE+3001)
        self.assertEqual(window['first_read_at_ms'], BASE+102)
        self.assertEqual(window['request_count'], 1)
        self.assertEqual(self.ingest.deliver('access', now_ms=BASE+3001)['status'], 'delivered')

    def test_truly_future_request_still_blocks_and_preserves_checkpoint(self):
        before = self.saved()
        self.append(line(103))
        self.assert_code('event_is_in_the_future', self.ingest.poll, 'access',
                         now_ms=BASE+100, read_clock_ms=lambda: BASE+102)
        self.assertEqual(self.saved(), before)
        self.assertEqual(self.ingest.status('access')['status'], 'needs_restart')

    def test_post_read_clock_backwards_blocks_without_checkpoint_commit(self):
        before = self.saved()
        self.append(line(100))
        self.assert_code('clock_went_backwards', self.ingest.poll, 'access',
                         now_ms=BASE+101, read_clock_ms=lambda: BASE+100)
        self.assertEqual(self.saved(), before)
        self.assertEqual(self.ingest.status('access')['status'], 'needs_restart')

    def test_post_read_availability_does_not_backdate_late_request(self):
        self.append(line(100))
        result = self.ingest.poll('access', now_ms=BASE+2999,
                                  read_clock_ms=lambda: BASE+3000)
        self.assertEqual(result['status'], 'degraded')
        self.assertEqual(self.ingest.status('access')['aggregate']['late_records'], 1)
        window = self.ingest.windows('access')[0]['payload']
        self.assertEqual(window['request_count'], 0)
        self.assertEqual(window['finalized_at_ms'], BASE+3000)

    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(LogIngestError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    @staticmethod
    def fault_at(target):
        def fault(step):
            if step == target:
                raise RuntimeError('simulated crash: '+step)
        return fault

    def test_partial_line_survives_new_store_and_reader_objects(self):
        value = line()
        self.append(value[:13])
        self.ingest.poll('access', now_ms=BASE+100)
        generation = self.ingest.status('access')['generation']
        self.restart()
        self.assertEqual(self.ingest.status('access')['reader']['buffered_bytes'], 13)
        self.append(value[13:])
        self.ingest.poll('access', now_ms=BASE+150)
        self.ingest.poll('access', now_ms=BASE+3000)
        self.assertEqual(self.ingest.status('access')['generation'], generation)
        self.assertEqual(self.ingest.windows('access')[0]['payload']['request_count'], 1)

    def test_open_window_restores_sums_and_does_not_count_identical_line_twice(self):
        value = line(100)
        self.append(value)
        self.ingest.poll('access', now_ms=BASE+150)
        self.restart()
        self.ingest.poll('access', now_ms=BASE+160)
        self.append(value)
        self.ingest.poll('access', now_ms=BASE+200)
        self.ingest.poll('access', now_ms=BASE+3000)
        window = self.ingest.windows('access')[0]['payload']
        self.assertEqual((window['request_count'], window['duration_sum_ms']), (2, 80))

    def test_poll_failure_before_commit_rolls_back_offset_buckets_and_windows(self):
        for step in ('after_read', 'after_windows', 'after_checkpoint'):
            with self.subTest(step=step):
                self.append(line(100)) if self.path.stat().st_size == 0 else None
                before = self.saved()
                self.ingest._fault_hook = self.fault_at(step)
                with self.assertRaises(RuntimeError):
                    self.ingest.poll('access', now_ms=BASE+2999)
                self.assertEqual(self.saved(), before)
                self.ingest._fault_hook = None
        self.restart()
        self.ingest.poll('access', now_ms=BASE+2999)
        self.ingest.poll('access', now_ms=BASE+3000)
        self.assertEqual(self.ingest.windows('access')[0]['payload']['request_count'], 1)

    def test_actual_window_insert_rolls_back_with_checkpoint(self):
        self.append(line())
        self.ingest.poll('access', now_ms=BASE+150)
        before = self.saved()
        self.ingest._fault_hook = self.fault_at('after_windows')
        with self.assertRaises(RuntimeError):
            self.ingest.poll('access', now_ms=BASE+3000)
        self.assertEqual(self.saved(), before)
        self.restart()
        self.ingest.poll('access', now_ms=BASE+3000)
        self.assertEqual(len(self.ingest.windows('access')), 1)

    def test_lost_poll_acknowledgement_does_not_repeat_counted_requests(self):
        self.append(line())
        self.ingest._fault_hook = self.fault_at('after_poll_commit')
        with self.assertRaises(RuntimeError):
            self.ingest.poll('access', now_ms=BASE+150)
        self.restart()
        self.ingest.poll('access', now_ms=BASE+200)
        self.ingest.poll('access', now_ms=BASE+3000)
        self.assertEqual(self.ingest.windows('access')[0]['payload']['request_count'], 1)

    def test_database_rejection_does_not_advance_file_position(self):
        self.append(line())
        before = self.saved()
        with self.store.connection() as db:
            db.execute("CREATE TRIGGER fail_cp BEFORE UPDATE ON log_checkpoints BEGIN SELECT RAISE(ABORT,'disk fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.ingest.poll('access', now_ms=BASE+150)
        self.assertEqual(self.saved(), before)
        with self.store.connection() as db:
            db.execute('DROP TRIGGER fail_cp')
        self.restart()
        self.ingest.poll('access', now_ms=BASE+200)
        self.assertEqual(self.ingest.status('access')['aggregate']['accepted_records'], 1)

    def test_delivery_rolls_back_model_observation_and_mark_together(self):
        self.prepare_window()
        for step in ('after_ingest', 'after_delivery_mark'):
            with self.subTest(step=step):
                before = self.saved()
                self.ingest._fault_hook = self.fault_at(step)
                with self.assertRaises(RuntimeError):
                    self.ingest.deliver('access', now_ms=BASE+3100)
                self.assertEqual(self.saved(), before)
        self.restart()
        result = self.ingest.deliver('access', now_ms=BASE+3200)
        self.assertEqual(result['status'], 'delivered')
        self.assertEqual(self.store.state('web')[0]['samples'], 1)

    def test_lost_delivery_ack_restores_exact_receipt_without_advancing_model(self):
        self.prepare_window()
        window = self.ingest.windows('access')[0]
        self.ingest._fault_hook = self.fault_at('after_delivery_commit')
        with self.assertRaises(RuntimeError):
            self.ingest.deliver('access', now_ms=BASE+3100)
        self.restart()
        before = self.saved()
        again = self.ingest.deliver('access', now_ms=BASE+3200, start_ms=BASE, expected_sha256=window['sha256'])
        self.assertEqual(again['status'], 'already_delivered')
        self.assertEqual(self.saved(), before)
        self.assertEqual(again['receipt']['samples'], 1)

    def test_notification_and_model_roll_back_together_then_repeat_is_read_only(self):
        self.prepare_window()
        original = ImmediateDetector.observe

        def alarm_fixture(detector, timestamp, values):
            original(detector, timestamp, values)
            # Exercise notification storage, not a claim that this row is anomalous.
            return [dict(timestamp=timestamp, decision_source_time=timestamp,
                         alarm=True, largest_residual_features=[FEATURES[0]])]

        before = self.saved()
        with patch.object(ImmediateDetector, 'observe', alarm_fixture):
            self.ingest._fault_hook = self.fault_at('after_delivery_mark')
            with self.assertRaises(RuntimeError):
                self.ingest.deliver('access', now_ms=BASE+3100)
            self.assertEqual(self.saved(), before)
            self.ingest._fault_hook = None
            result = self.ingest.deliver('access', now_ms=BASE+3200)
        self.assertEqual(result['receipt']['notifications'], 1)
        saved = self.saved()
        with patch.object(ImmediateDetector, 'observe', side_effect=AssertionError('Duplicate called detector')):
            self.assertEqual(self.ingest.deliver('access', now_ms=BASE+3300, start_ms=BASE)['status'], 'already_delivered')
        self.assertEqual(self.saved(), saved)

    def test_changed_content_for_same_key_stops_source(self):
        self.prepare_window()
        self.assert_code('window_hash_mismatch', self.ingest.deliver, 'access',
                         now_ms=BASE+3100, start_ms=BASE, expected_sha256='0'*64)
        self.assertEqual(self.ingest.status('access')['status'], 'needs_restart')
        self.assertEqual(self.store.state('web')[0]['samples'], 0)

    def test_saved_window_tampering_is_not_an_idempotent_repeat(self):
        self.prepare_window()
        with self.store.connection() as db:
            db.execute("UPDATE log_windows SET payload='{}'")
        self.assert_code('window_hash_mismatch', self.ingest.deliver, 'access', now_ms=BASE+3100)

    def test_corrupt_checkpoint_stops_instead_of_starting_at_eof(self):
        self.append(line())
        with self.store.connection() as db:
            db.execute("UPDATE log_checkpoints SET state='{}'")
        self.assert_code('checkpoint_or_configuration_mismatch', self.ingest.poll, 'access', now_ms=BASE+150)
        with self.store.connection() as db:
            self.assertEqual(db.execute('SELECT status FROM log_sources').fetchone()[0], 'needs_restart')

    def test_wrong_version_even_with_matching_hash_is_rejected(self):
        with self.store.connection() as db:
            row = db.execute('SELECT state FROM log_checkpoints').fetchone()[0]
            state = json.loads(row)
            state['version'] = 99
            text = canonical(state)
            db.execute('UPDATE log_checkpoints SET state=?,sha256=?', (text, digest(text)))
        self.assert_code('checkpoint_or_configuration_mismatch', self.ingest.poll, 'access', now_ms=BASE+150)

    def test_empty_and_subsequent_degraded_windows_never_enter_model(self):
        self.prepare_window()
        self.ingest.poll('access', now_ms=BASE+5000)
        for _ in range(3):
            self.ingest.deliver('access', now_ms=BASE+5100)
        self.assertEqual(self.store.state('web')[0]['samples'], 1)
        self.assertEqual(self.ingest.status('access')['windows'], {'delivered': 1, 'skipped': 2})
        self.restart()
        self.append(line(5100))
        self.ingest.poll('access', now_ms=BASE+5150)
        self.ingest.poll('access', now_ms=BASE+8000)
        for _ in range(3):
            self.ingest.deliver('access', now_ms=BASE+8100)
        self.assertEqual(self.store.state('web')[0]['samples'], 1)
        self.assertEqual(self.ingest.status('access')['status'], 'degraded')

    def test_parse_error_and_late_record_are_persisted_quality_failures(self):
        self.append(b'broken\n')
        self.ingest.poll('access', now_ms=BASE+100)
        self.restart()
        self.append(line(100))
        self.ingest.poll('access', now_ms=BASE+3500)
        self.assertEqual(self.ingest.status('access')['aggregate']['late_records'], 1)
        self.assertEqual(self.ingest.status('access')['aggregate']['parse_errors']['format_mismatch'], 1)
        self.ingest.deliver('access', now_ms=BASE+3600)
        self.assertEqual(self.store.state('web')[0]['samples'], 0)

    def test_file_replacement_after_restart_does_not_start_a_new_generation(self):
        generation = self.ingest.status('access')['generation']
        replacement = self.path.with_name('replacement.log')
        replacement.write_bytes(line())
        replacement.replace(self.path)
        self.restart()
        self.assert_code('file_replaced', self.ingest.poll, 'access', now_ms=BASE+150)
        self.assertEqual(self.ingest.status('access')['generation'], generation)
        self.assert_code('source_needs_restart', self.ingest.deliver, 'access', now_ms=BASE+200)

    def test_ready_queue_applies_backpressure_without_losing_checkpoint(self):
        self.prepare_window()
        before = self.saved()
        self.append(line(3100))
        with patch('monitor_service.log_ingest.MAX_READY_WINDOWS', 1):
            result = self.ingest.poll('access', now_ms=BASE+3150)
        self.assertEqual(result['status'], 'backpressure')
        self.assertEqual(self.saved(), before)
        self.ingest.deliver('access', now_ms=BASE+3200)
        self.ingest.poll('access', now_ms=BASE+3250)
        self.assertEqual(self.ingest.status('access')['aggregate']['accepted_records'], 2)

    def test_public_ingest_cannot_mix_other_metrics_into_bound_model(self):
        with self.assertRaises(Conflict):
            self.store.ingest('web', {'observations': [{'timestamp': 1, 'values': dict(zip(FEATURES, [1, 1, 0]))}]})
        self.assertEqual(self.store.state('web')[0]['samples'], 0)

    def test_source_requires_new_model_and_unique_binding(self):
        with self.assertRaises(Conflict):
            self.ingest.attach('second', 'web', self.path, start_ms=BASE)
        self.store.register(dict(id='wrong', features=['cpu', 'ram']))
        with self.assertRaises(ValueError):
            self.ingest.attach('wrong', 'wrong', self.path, start_ms=BASE)
        with self.store.connection() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM log_sources').fetchone()[0], 1)

    def test_delivery_order_and_availability_are_enforced(self):
        self.prepare_window()
        self.ingest.poll('access', now_ms=BASE+4000)
        with self.assertRaises(Conflict):
            self.ingest.deliver('access', now_ms=BASE+4100, start_ms=BASE+1000)
        self.assert_code('delivery_clock_went_backwards', self.ingest.deliver, 'access', now_ms=BASE+2999)

    def test_foreign_model_change_is_detected(self):
        self.prepare_window()
        with self.store.connection() as db:
            state = json.loads(db.execute("SELECT state FROM hosts WHERE id='web'").fetchone()[0])
            state['seen'] += 1
            db.execute("UPDATE hosts SET state=? WHERE id='web'", (encode(state),))
        self.assert_code('model_changed_outside_log_delivery', self.ingest.deliver, 'access', now_ms=BASE+3100)

    def test_concurrent_instances_do_not_deliver_same_window_twice(self):
        self.prepare_window()
        other = LogIngestor(Store(self.db_path))
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda obj: obj.deliver('access', now_ms=BASE+3100, start_ms=BASE), [self.ingest, other]))
        self.assertEqual(sorted(r['status'] for r in results), ['already_delivered', 'delivered'])
        self.assertEqual(self.store.state('web')[0]['samples'], 1)

    def test_concurrent_polls_use_committed_state_not_stale_in_memory_copy(self):
        self.append(line())
        other = LogIngestor(Store(self.db_path))
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda obj: obj.poll('access', now_ms=BASE+150), [self.ingest, other]))
        self.assertEqual(self.ingest.status('access')['aggregate']['accepted_records'], 1)

    def test_checkpoint_roundtrip_checks_pending_bytes_buckets_and_capacities(self):
        reader = LogTailer(self.path, source_id='sample')
        aggregate = LogAggregator(BASE)
        value = line()
        self.append(value[:12])
        reader.poll(BASE+100)
        state = pack(reader, aggregate)
        restored_reader, restored_aggregate = unpack(json.loads(canonical(state)))
        self.assertEqual(pack(restored_reader, restored_aggregate), state)
        for key, value in (('_buffer', 'ff'*5000), ('_anchor', ''), ('_read_offset', -1)):
            bad = copy.deepcopy(state)
            bad['reader'][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                unpack(bad)

    def test_full_warmup_and_model_update_survive_duplicate_deliveries(self):
        # Disposable controlled log, not SMD or a new quality experiment.
        reference_rows = []
        for second in range(913):
            count = 1+second % 4
            durations = [20+(second*7+index*3) % 101 for index in range(count)]
            statuses = [503 if (second+index) % 13 == 0 else 200 for index in range(count)]
            for index in range(count):
                self.append(line(second*1000+100+index*10, statuses[index], durations[index]))
            self.ingest.poll('access', now_ms=BASE+second*1000+200)
            reference_rows.append(dict(timestamp=(BASE+(second+1)*1000)/1000,
                values=dict(zip(FEATURES, [float(count), sum(durations)/count,
                            sum(status == 503 for status in statuses)/count]))))
        self.ingest.poll('access', now_ms=BASE+915000)
        for index in range(913):
            self.ingest.deliver('access', now_ms=BASE+915100)
            if index in (0, 895, 911, 912):
                self.restart()
                before = self.saved()
                self.assertEqual(self.ingest.deliver('access', now_ms=BASE+915100,
                                 start_ms=BASE+index*1000)['status'], 'already_delivered')
                self.assertEqual(self.saved(), before)
        self.store.register(dict(id='reference', source='external', features=list(FEATURES),
                                 settings=dict(components=1, processing_mode=MODE)))
        for offset in range(0, len(reference_rows), 128):
            self.store.ingest('reference', {'observations': reference_rows[offset:offset+128]})
        with self.store.connection() as db:
            left = db.execute("SELECT state FROM hosts WHERE id='web'").fetchone()[0]
            right = db.execute("SELECT state FROM hosts WHERE id='reference'").fetchone()[0]
            self.assertEqual(left, right)
            self.assertEqual(db.execute("SELECT count(*) FROM observations WHERE host='web'").fetchone()[0], 913)
            decisions = db.execute("SELECT result FROM observations WHERE host='web' AND result IS NOT NULL ORDER BY ts").fetchall()
            self.assertEqual(len(decisions), 17)
            for row, ref in zip(decisions, db.execute("SELECT result FROM observations WHERE host='reference' AND result IS NOT NULL ORDER BY ts")):
                actual, expected = json.loads(row[0]), json.loads(ref[0])
                actual.pop('emitted_at'); expected.pop('emitted_at')
                self.assertEqual(actual, expected)
        self.assertEqual(self.store.state('web')[0]['pending'], 1)
        self.assertEqual(self.ingest.status('access')['windows'], {'delivered': 913})


if __name__ == '__main__':
    unittest.main()
