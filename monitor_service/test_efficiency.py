"""Technical fixtures only: no real collection and no discovery experiment."""
from contextlib import redirect_stdout
from io import StringIO
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from .efficiency_baselines import CausalWindow, SimpleComparisons, measurement_summary, METHODS, WARMUP
from .efficiency_schedule import Schedule, post_schedule
from .efficiency_pilot import main, perform, code_snapshot, collect


def row(index, cpu=None, count=None, timestamp=None):
    # Time windows span several counts/rates; independent deterministic fixture.
    return {'timestamp': 1000 + index if timestamp is None else timestamp,
            'cpu_seconds': index * .01 if cpu is None else cpu,
            'requests_completed': index * 2 + index // 32 if count is None else count,
            'memory_mib': 30 + (index % 7) * .01, 'active_requests': index % 2}


def warm(detector):
    for i in range(WARMUP):
        assert detector.observe(row(i)) is None
    return detector


class WindowTests(unittest.TestCase):
    def test_common_endpoints_exact_cpu_and_completions(self):
        window = CausalWindow()
        for i in range(11):
            result = window.observe(row(i))
        self.assertEqual(result['window_start'], 1000)
        self.assertEqual(result['duration_seconds'], 10)
        self.assertAlmostEqual(result['cpu_ms'], 100)
        self.assertEqual(result['completed'], 20)
        self.assertAlmostEqual(result['features'][0], 1.)
        self.assertAlmostEqual(result['features'][3], 2.)

    def test_irregular_endpoints_no_interpolation_and_weighted_memory(self):
        window = CausalWindow()
        for i, ts in enumerate((1000, 1002, 1004, 1006, 1008, 1011)):
            result = window.observe(row(i, cpu=i * .01, count=i * 2, timestamp=ts))
        self.assertEqual(result['duration_seconds'], 11)
        expected = sum(dt * row(i)['memory_mib'] for dt, i in ((2, 1), (2, 2), (2, 3), (2, 4), (3, 5))) / 11
        self.assertAlmostEqual(result['features'][1], expected)

    def test_gap_reset_and_old_endpoint_are_invalid_not_normal(self):
        for changed, reason in ((row(10, timestamp=1020), 'endpoint_too_old'),
                                (row(10, cpu=0), 'counter_reset'),
                                (row(10, count=0), 'counter_reset')):
            window = CausalWindow()
            for i in range(10):
                window.observe(row(i))
            result = window.observe(changed)
            # A 20s endpoint selects t=1009, leaving an 11s gap.
            self.assertFalse(result['valid'])
            self.assertIn(result['reason'], (reason, 'measurement_gap'))
        window = CausalWindow()
        for i in (0, 1, 2, 6, 7, 8, 9, 10):
            result = window.observe(row(i))
        self.assertEqual(result['reason'], 'measurement_gap')

    def test_zero_completions_are_not_divided_or_called_normal(self):
        window = CausalWindow()
        for i in range(11):
            result = window.observe(row(i, count=0))
        self.assertTrue(result['valid'])
        self.assertFalse(result['ratio_eligible'])

    def test_outcome_and_work_metadata_are_ignored(self):
        a, b = CausalWindow(), CausalWindow()
        for i in range(20):
            original = row(i)
            altered = dict(original, served_mode='extra_cpu', extra_cpu_seconds=10**9,
                           client_target_rps=999, http_status=503, response_time_ms=1e9)
            self.assertEqual(a.observe(original), b.observe(altered))

    def test_invalid_inputs_and_order_are_rejected(self):
        for key, value in (('cpu_seconds', float('nan')), ('requests_completed', 1.5),
                           ('timestamp', 0), ('active_requests', True), ('memory_mib', -1)):
            with self.assertRaises(ValueError):
                CausalWindow().observe(dict(row(0), **{key: value}))
        window = CausalWindow()
        window.observe(row(0))
        with self.assertRaises(ValueError):
            window.observe(row(0))

    def test_measurement_gate_excludes_startup_and_exposes_zero_cpu(self):
        report = measurement_summary(row(i) for i in range(50))
        self.assertEqual(report['candidate_windows'], 40)
        self.assertTrue(report['coverage_gate_passed'])
        empty = measurement_summary(row(i, count=0) for i in range(50))
        self.assertFalse(empty['coverage_gate_passed'])
        self.assertIsNone(measurement_summary([row(0)])['eligible_fraction'])
        zeros = measurement_summary(row(i, cpu=0) for i in range(50))
        self.assertEqual(zeros['positive_cpu_windows'], 0)
        # This is the coverage gate only, not a certificate of CPU precision.
        self.assertTrue(zeros['coverage_gate_passed'])


class BaselineTests(unittest.TestCase):
    def test_exact_warmup_and_three_scores(self):
        detector = warm(SimpleComparisons())
        result = detector.observe(row(WARMUP))
        self.assertEqual(set(result['decisions']), set(METHODS))
        self.assertAlmostEqual(result['decisions']['cpu_per_request']['score'],
                               result['window']['cpu_ms'] / result['window']['completed'])
        self.assertTrue(all(d['threshold'] >= 0 for d in result['decisions'].values()))
        self.assertEqual(len(detector.pending), 1)

    def test_own_block_cannot_set_its_threshold(self):
        detector = warm(SimpleComparisons())
        old = dict(detector.cuts)
        for offset in range(16):
            i = WARMUP + offset
            result = detector.observe(row(i, cpu=i * .01 + (offset + 1) * 10))
            self.assertEqual({m: d['threshold'] for m, d in result['decisions'].items()}, old)
            self.assertTrue(result['decisions']['cpu_per_request']['alarm'])
        self.assertEqual(len(detector.pending), 0)
        self.assertNotEqual(detector.cuts, old)
        following = detector.observe(row(WARMUP + 16, cpu=(WARMUP + 16) * .01 + 170))
        self.assertEqual({m: d['threshold'] for m, d in following['decisions'].items()}, detector.cuts)

    def test_reproducible_prefix_and_metadata_independence(self):
        a, b = SimpleComparisons(), SimpleComparisons()
        for i in range(WARMUP + 35):
            left = a.observe(row(i))
            right = b.observe(dict(row(i), extra_work_iterations=10**9, served_mode='error'))
            if left is not None:
                for method in METHODS:
                    for key in ('score', 'threshold', 'alarm'):
                        self.assertEqual(left['decisions'][method][key], right['decisions'][method][key])
        frozen = dict(a.cuts)
        # A different future cannot change the already-returned record.
        a.observe(row(WARMUP + 35, cpu=100))
        self.assertEqual(frozen, b.cuts)

    def test_invalid_window_and_ratio_have_none_alarm(self):
        detector = warm(SimpleComparisons())
        result = detector.observe(row(WARMUP, timestamp=3000))
        self.assertTrue(all(d['alarm'] is None for d in result['decisions'].values()))
        valid = {'valid': True, 'features': [1, 30, 0, 0], 'ratio_eligible': False,
                 'completed': 0, 'cpu_ms': 5}
        scores, _, outside = detector.scores(valid)
        self.assertIsNone(scores['cpu_per_request'])
        self.assertTrue(outside)

    def test_constant_count_fit_is_rejected_instead_of_fabricated_model(self):
        detector = SimpleComparisons()
        with self.assertRaisesRegex(ValueError, 'Distinct'):
            for i in range(256):
                detector.observe(row(i, count=i * 2))


class ScheduleTests(unittest.TestCase):
    def test_exact_timeline_and_reversed_order(self):
        for order in (1, 2):
            schedule = post_schedule(order)
            self.assertEqual(schedule[-1].at, 390)
            works = [a for a in schedule if a.kind == 'extra_cpu']
            self.assertEqual([a.seconds for a in works], [10, 30] if order == 1 else [30, 10])
            self.assertEqual(works[1].at - (works[0].at + works[0].seconds), 60)
            loads = [a.value for a in schedule if a.kind == 'load' and a.value != 2]
            self.assertEqual(loads, [4, 8] if order == 1 else [8, 4])

    def test_training_cycles_and_never_injects_work_before_ready(self):
        scheduler = Schedule(1)
        actions = []
        for i in range(896):
            actions.extend(scheduler.tick(i, 'warmup', i))
        self.assertTrue(all(a.kind == 'load' for a in actions))
        self.assertEqual([a.value for a in actions[:6]], [2, 4, 8, 2, 4, 8])
        self.assertEqual(scheduler.tick(896, 'monitoring', 896)[0].kind, 'normal')
        self.assertEqual(scheduler.tick(950, 'monitoring', 956)[0].value, 4)

    def test_no_repeated_command_or_compression_after_missed_deadline(self):
        scheduler = Schedule(1)
        scheduler.tick(0, 'warmup', 0)
        self.assertEqual(scheduler.tick(0, 'warmup', .1), [])
        with self.assertRaisesRegex(ValueError, 'milestone'):
            scheduler.tick(96, 'warmup', 1)
        scheduler = Schedule(1)
        scheduler.tick(896, 'monitoring', 0)
        with self.assertRaisesRegex(ValueError, 'late'):
            scheduler.tick(960, 'monitoring', 62)
        with self.assertRaisesRegex(ValueError, 'transition'):
            Schedule(1).tick(897, 'monitoring', 0)

    def test_finished_schedule_stays_finished_and_progress_must_increase(self):
        scheduler = Schedule(2)
        for action in post_schedule(2):
            result = scheduler.tick(896, 'monitoring', action.at)
            self.assertEqual(result, [action])
        self.assertTrue(scheduler.finished)
        self.assertEqual(scheduler.tick(896, 'monitoring', 391), [])
        with self.assertRaises(ValueError):
            scheduler.tick(895, 'monitoring', 392)

    def test_default_preview_does_not_collect_or_create_output(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'must-not-exist'
            with patch('monitor_service.efficiency_pilot.collect') as collect, redirect_stdout(StringIO()) as stream:
                main(['--order', '2', '--output', str(target)])
            collect.assert_not_called()
            self.assertFalse(target.exists())
            self.assertEqual(json.loads(stream.getvalue())['order'], 2)

    def test_invalid_order_is_rejected_before_any_collection(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'absent'
            with self.assertRaises(ValueError):
                collect(target, 3)
            self.assertFalse(target.exists())

    def test_archive_is_code_only_no_user_data_and_has_matching_hashes(self):
        import hashlib
        import zipfile
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            code_snapshot(root)
            manifest = json.loads((root / 'source_manifest.json').read_text(encoding='utf-8'))
            with zipfile.ZipFile(root / 'code.zip') as archive:
                self.assertEqual(set(archive.namelist()), {r['path'] for r in manifest['files']})
                for record in manifest['files']:
                    self.assertEqual(hashlib.sha256(archive.read(record['path'])).hexdigest(), record['sha256'])
                    self.assertNotIn('sqlite', record['path'])

    def test_perform_dispatches_only_expected_bench_controls(self):
        from unittest.mock import Mock
        bench = Mock()
        for action in post_schedule(1):
            perform(bench, action)
        bench.start.assert_not_called()
        bench.set_load.assert_any_call(8, 30)
        bench.control.assert_any_call('extra_cpu', 10)
        bench.stop.assert_called_once()


class RunnerTests(unittest.TestCase):
    """Replace the clock, collector and child app. No socket or workload."""
    def run_fixture(self, root, order=1, failure=None):
        import threading
        from unittest.mock import Mock
        now = [0.]
        instance = Mock()
        instance.lock = threading.RLock()
        instance.stop_event = threading.Event()
        instance.current = {'id': 'fixture', 'host': 'fixture', 'running': True, 'error': None}
        instance.thread = None
        instance.start.return_value = dict(instance.current)
        def state(host):
            count = int(now[0])
            return [{'samples': count, 'phase': 'warmup' if count < 896 else 'monitoring'}]
        instance.store.state.side_effect = state
        for method in ('set_load', 'control', 'stop'):
            getattr(instance, method).return_value = {'fixture': True}
        if failure == 'startup':
            instance.start.side_effect = RuntimeError('fixture startup failure')
            instance.current = None
        if failure == 'interrupt':
            instance.control.side_effect = KeyboardInterrupt()
        if failure == 'cleanup':
            instance.close.side_effect = RuntimeError('fixture cleanup failure')
        def advance(seconds):
            now[0] += 1.
        with patch('monitor_service.bench.Bench', return_value=instance), patch('monitor_service.store.Store'):
            result = collect(root, order, clock=lambda: now[0], sleep=advance)
        return result, instance

    def test_full_schedule_with_virtual_clock_and_isolated_folder(self):
        for order in (1, 2):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / 'new'
                result, bench = self.run_fixture(root, order)
                self.assertEqual(result['status'], 'collection_complete_awaiting_measurement_audit')
                self.assertFalse(result['quality_evaluated'])
                log = [json.loads(line) for line in (root / 'schedule_log.jsonl').read_text(encoding='utf-8').splitlines()]
                self.assertEqual(log[-1]['action']['kind'], 'stop')
                self.assertEqual(log[-1]['post_elapsed_seconds'], 390)
                extras = [r for r in log if r['action']['kind'] == 'extra_cpu']
                self.assertEqual([r['action']['seconds'] for r in extras], [10, 30] if order == 1 else [30, 10])
                bench.close.assert_called_once()
                self.assertFalse((root / 'quality.json').exists())

    def test_existing_output_is_not_overwritten_or_started(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            marker = root / 'keep.txt'
            marker.write_text('original', encoding='utf-8')
            with patch('monitor_service.bench.Bench') as bench, self.assertRaises(FileExistsError):
                collect(root, 1)
            bench.assert_not_called()
            self.assertEqual(marker.read_text(encoding='utf-8'), 'original')

    def test_startup_failure_preserves_report_and_cleans_up(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'new'
            with self.assertRaisesRegex(RuntimeError, 'startup'):
                self.run_fixture(root, failure='startup')
            report = json.loads((root / 'collection_status.json').read_text(encoding='utf-8'))
            self.assertEqual(report['status'], 'failed')
            self.assertFalse(report['quality_evaluated'])

    def test_interrupt_and_cleanup_failure_cannot_be_called_complete(self):
        for failure in ('interrupt', 'cleanup'):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / 'new'
                with self.assertRaises((KeyboardInterrupt, RuntimeError)):
                    self.run_fixture(root, failure=failure)
                report = json.loads((root / 'collection_status.json').read_text(encoding='utf-8'))
                self.assertIn(report['status'], ('interrupted', 'failed'))
                self.assertFalse(report['quality_evaluated'])


if __name__ == '__main__':
    unittest.main()
