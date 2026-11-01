"""Independent numeric expectations and deterministic timing/quality cases."""
from dataclasses import FrozenInstanceError
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from .log_schema import AccessRecord, WindowNotEligible, FEATURES
from .log_parser import parse_line
from .log_aggregation import LogAggregator, AggregationError

BASE = 1792490400000
FIXTURES = Path(__file__).resolve().parent/'examples/logs/v1'


def request(offset, status=200, duration=10):
    return AccessRecord(BASE+offset, status, duration)


class LogAggregationTests(unittest.TestCase):
    def test_hand_calculated_file_end_to_end(self):
        expected = json.loads((FIXTURES/'expected.json').read_text(encoding='utf-8'))
        engine = LogAggregator(expected['start_ms'])
        count = 0
        # Iterate lines of a small fixture; this is not the future live tailer.
        with (FIXTURES/'access.log').open('rb') as stream:
            for line in stream:
                row = parse_line(line)
                self.assertEqual(engine.add(row, read_at_ms=row.timestamp_ms+50), 'accepted')
                count += 1
        windows = engine.advance(BASE+6000)
        self.assertEqual(count, expected['records'])
        self.assertEqual(len(windows), len(expected['windows']))
        for actual, wanted in zip(windows, expected['windows']):
            for key in ('start_ms', 'request_count', 'error_5xx_count', 'error_4xx_count',
                        'duration_sum_ms', 'quality'):
                self.assertEqual(getattr(actual, key), wanted[key], key)
            for key in FEATURES:
                value = actual.metrics()[key]
                if wanted[key] is None:
                    self.assertIsNone(value)
                else:
                    self.assertAlmostEqual(value, wanted[key])
        self.assertEqual(engine.diagnostics()['accepted_records'], 6)

    def test_window_boundaries_are_half_open_integer_milliseconds(self):
        engine = LogAggregator(BASE)
        engine.add(request(999, 500, 30), read_at_ms=BASE+999)
        engine.add(request(1000, 200, 70), read_at_ms=BASE+1000)
        a, b = engine.advance(BASE+4000)
        self.assertEqual((a.request_count, b.request_count), (1, 1))
        self.assertEqual((a.error_5xx_count, b.error_5xx_count), (1, 0))
        self.assertEqual((a.duration_sum_ms, b.duration_sum_ms), (30, 70))

    def test_full_window_waits_until_two_second_grace_ends(self):
        engine = LogAggregator(BASE)
        engine.add(request(100), read_at_ms=BASE+100)
        self.assertEqual(engine.advance(BASE+2999), [])
        window, = engine.advance(BASE+3000)
        self.assertEqual(window.end_ms, BASE+1000)
        self.assertEqual(window.finalized_at_ms, BASE+3000)

    def test_arrival_at_exact_deadline_is_late_even_without_advance(self):
        engine = LogAggregator(BASE)
        self.assertEqual(engine.add(request(999), read_at_ms=BASE+2999), 'accepted')
        self.assertEqual(engine.add(request(999), read_at_ms=BASE+3000), 'late')
        window, = engine.advance(BASE+3000)
        self.assertEqual(window.request_count, 1)
        self.assertEqual(engine.diagnostics()['late_records'], 1)
        self.assertEqual(window.quality, 'degraded')

    def test_out_of_order_events_within_grace_enter_their_own_windows(self):
        engine = LogAggregator(BASE)
        engine.add(request(1100, 200, 20), read_at_ms=BASE+1200)
        engine.add(request(900, 503, 40), read_at_ms=BASE+1300)
        first, second = engine.advance(BASE+4000)
        self.assertEqual((first.duration_sum_ms, second.duration_sum_ms), (40, 20))
        self.assertTrue(first.eligible_for_model and second.eligible_for_model)

    def test_identical_timestamp_and_text_are_distinct_requests(self):
        engine = LogAggregator(BASE)
        row = request(100, 503, 40)
        engine.add(row, read_at_ms=BASE+100)
        engine.add(row, read_at_ms=BASE+100)
        window, = engine.advance(BASE+3000)
        self.assertEqual((window.request_count, window.error_5xx_count), (2, 2))
        self.assertEqual(window.duration_sum_ms, 80)

    def test_4xx_is_counted_separately_from_5xx(self):
        engine = LogAggregator(BASE)
        for i, status in enumerate((200, 301, 400, 499, 500, 599)):
            engine.add(request(100+i, status), read_at_ms=BASE+100+i)
        window, = engine.advance(BASE+3000)
        self.assertEqual((window.request_count, window.error_4xx_count, window.error_5xx_count), (6, 2, 2))
        self.assertAlmostEqual(window.server_error_fraction, 1/3)

    def test_empty_window_has_no_fake_latency_or_fraction(self):
        engine = LogAggregator(BASE)
        window, = engine.advance(BASE+3000)
        self.assertEqual(window.completed_rps, 0)
        self.assertIsNone(window.mean_request_ms)
        self.assertIsNone(window.server_error_fraction)
        self.assertEqual(window.quality, 'empty')
        with self.assertRaises(WindowNotEligible):
            window.as_observation()

    def test_gap_latches_session_pause_without_stopping_counters(self):
        engine = LogAggregator(BASE)
        engine.advance(BASE+3000)  # Empty first second.
        engine.add(request(3100, 200, 50), read_at_ms=BASE+3100)
        windows = engine.advance(BASE+6000)
        self.assertEqual(windows[-1].request_count, 1)
        self.assertEqual(windows[-1].quality, 'degraded')
        self.assertTrue(engine.requires_restart)
        with self.assertRaises(WindowNotEligible):
            windows[-1].as_observation()

    def test_explicit_new_session_can_be_used_after_gap(self):
        old = LogAggregator(BASE)
        old.advance(BASE+3000)
        new = LogAggregator(BASE+3000)
        new.add(request(3100), read_at_ms=BASE+3100)
        window, = new.advance(BASE+6000)
        self.assertTrue(window.eligible_for_model)
        self.assertTrue(old.requires_restart)

    def test_parse_failure_does_not_guess_a_request_or_timestamp(self):
        engine = LogAggregator(BASE)
        engine.add(request(100), read_at_ms=BASE+100)
        engine.note_parse_error('format_mismatch', read_at_ms=BASE+200)
        window, = engine.advance(BASE+3000)
        self.assertEqual(window.request_count, 1)
        self.assertEqual(window.quality, 'degraded')
        self.assertEqual(engine.diagnostics()['parse_errors']['format_mismatch'], 1)
        with self.assertRaises(WindowNotEligible):
            window.as_observation()

    def test_late_record_never_rewrites_already_emitted_window(self):
        engine = LogAggregator(BASE)
        engine.add(request(100), read_at_ms=BASE+100)
        window, = engine.advance(BASE+3000)
        saved = window.as_dict()
        self.assertEqual(engine.add(request(200, 503), read_at_ms=BASE+3100), 'late')
        self.assertEqual(window.as_dict(), saved)
        self.assertTrue(engine.requires_restart)
        with self.assertRaises(FrozenInstanceError):
            window.request_count = 99

    def test_no_duplicate_window_on_repeated_advance(self):
        engine = LogAggregator(BASE)
        engine.add(request(100), read_at_ms=BASE+100)
        self.assertEqual(len(engine.advance(BASE+3000)), 1)
        self.assertEqual(engine.advance(BASE+3000), [])
        self.assertEqual(engine.diagnostics()['emitted_windows'], 1)

    def test_actual_finalization_time_is_not_backdated(self):
        engine = LogAggregator(BASE)
        engine.add(request(100), read_at_ms=BASE+100)
        window, = engine.advance(BASE+9000, max_windows=1)
        self.assertEqual(window.finalized_at_ms, BASE+9000)
        self.assertEqual(window.as_observation()['timestamp'], (BASE+1000)/1000)

    def test_read_times_are_provenance_not_input_features(self):
        engine = LogAggregator(BASE)
        engine.add(request(100, duration=20), read_at_ms=BASE+200)
        engine.add(request(900, duration=40), read_at_ms=BASE+1500)
        window, = engine.advance(BASE+3000)
        self.assertEqual((window.first_read_at_ms, window.last_read_at_ms), (BASE+200, BASE+1500))
        payload = window.as_observation()
        self.assertEqual(tuple(payload['values']), FEATURES)
        self.assertEqual(payload['values'], dict(completed_rps=2., mean_request_ms=30., server_error_fraction=0.))
        self.assertNotIn('finalized_at_ms', payload['values'])

    def test_clock_rollback_and_future_event_do_not_mutate_counts(self):
        engine = LogAggregator(BASE)
        engine.add(request(100), read_at_ms=BASE+200)
        before = engine.diagnostics()
        for action, code in ((lambda: engine.advance(BASE+199), 'clock_went_backwards'),
                             (lambda: engine.add(request(500), read_at_ms=BASE+400), 'event_is_in_the_future')):
            with self.subTest(code=code):
                with self.assertRaises(AggregationError) as caught:
                    action()
                self.assertEqual(caught.exception.code, code)
                self.assertEqual(engine.diagnostics(), before)

    def test_bad_types_and_configuration_are_rejected(self):
        for start in (BASE+1, -1000, True, float(BASE)):
            with self.subTest(start=start):
                with self.assertRaises(ValueError):
                    LogAggregator(start)
        for capacity in (0, 9, True, 1.5):
            with self.subTest(capacity=capacity):
                with self.assertRaises(ValueError):
                    LogAggregator(BASE, max_pending_windows=capacity)
        engine = LogAggregator(BASE)
        with self.assertRaises(TypeError):
            engine.add({'timestamp_ms': BASE}, read_at_ms=BASE)
        for clock in (True, float(BASE), -1):
            with self.subTest(clock=clock):
                with self.assertRaises(ValueError):
                    engine.advance(clock)
        with self.assertRaises(ValueError):
            engine.note_parse_error('guessed_issue', read_at_ms=BASE)

    def test_before_session_records_do_not_enter_first_window(self):
        engine = LogAggregator(BASE+1000)
        self.assertEqual(engine.add(request(999), read_at_ms=BASE+1000), 'before_start')
        engine.add(request(1000), read_at_ms=BASE+1000)
        window, = engine.advance(BASE+4000)
        self.assertEqual(window.request_count, 1)
        self.assertEqual(engine.diagnostics()['before_start_records'], 1)
        self.assertTrue(window.eligible_for_model)

    def test_pending_capacity_backpressure_is_explicit_and_atomic(self):
        engine = LogAggregator(BASE, max_pending_windows=1)
        engine.add(request(100), read_at_ms=BASE+100)
        before = engine.diagnostics()
        with self.assertRaises(AggregationError) as caught:
            engine.add(request(1100), read_at_ms=BASE+1100)
        self.assertEqual(caught.exception.code, 'pending_capacity_exceeded')
        self.assertEqual(engine.diagnostics(), before)

    def test_request_count_capacity_does_not_drop_or_partially_count(self):
        engine = LogAggregator(BASE)
        with patch('monitor_service.log_aggregation.MAX_REQUESTS_PER_WINDOW', 2):
            engine.add(request(100), read_at_ms=BASE+100)
            engine.add(request(200), read_at_ms=BASE+200)
            before = engine.diagnostics()
            with self.assertRaises(AggregationError) as caught:
                engine.add(request(300, 500), read_at_ms=BASE+300)
            self.assertEqual(caught.exception.code, 'window_capacity_exceeded')
            self.assertEqual(engine.diagnostics(), before)
        window, = engine.advance(BASE+3000)
        self.assertEqual((window.request_count, window.error_5xx_count), (2, 0))

    def test_large_clock_gap_has_bounded_output_and_can_be_drained(self):
        engine = LogAggregator(BASE)
        for call in range(3):
            windows = engine.advance(BASE+60000, max_windows=2)
            self.assertEqual(len(windows), 2)
            self.assertEqual(windows[0].start_ms, BASE+call*2000)
        self.assertEqual(engine.diagnostics()['emitted_windows'], 6)
        with self.assertRaises(ValueError):
            engine.advance(BASE+60000, max_windows=257)

    def test_many_records_keep_only_open_buckets_not_request_lists(self):
        engine = LogAggregator(BASE)
        emitted = accepted = peak = 0
        for second in range(5000):
            for offset in (100, 700):
                self.assertEqual(engine.add(request(second*1000+offset),
                                            read_at_ms=BASE+second*1000+offset), 'accepted')
                accepted += 1
            peak = max(peak, engine.pending_windows)
            for window in engine.advance(BASE+(second+1)*1000):
                self.assertEqual(window.request_count, 2)
                self.assertTrue(window.eligible_for_model)
                emitted += 1
        emitted += len(engine.advance(BASE+5002000))
        self.assertEqual((accepted, emitted), (10000, 5000))
        self.assertLessEqual(peak, 3)
        self.assertEqual(engine.pending_windows, 0)
        self.assertFalse(engine.requires_restart)

    def test_future_input_cannot_change_a_saved_past_window(self):
        outputs = []
        for future_status in (200, 503):
            engine = LogAggregator(BASE)
            engine.add(request(100), read_at_ms=BASE+100)
            past, = engine.advance(BASE+3000)
            engine.add(request(3100, future_status), read_at_ms=BASE+3100)
            engine.advance(BASE+6000)
            outputs.append(past.as_dict())
        self.assertEqual(outputs[0], outputs[1])


if __name__ == '__main__':
    unittest.main()
