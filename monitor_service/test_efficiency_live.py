"""Short real-child wiring check, explicitly NOT a pilot collection.

896 artificial bootstrap rows replace the long normal training ONLY in these
fixtures. Post-schedule deadlines are stepped by the test, not waited in real
time. Actual commands/requests/metrics/shutdown use the unmodified real Bench.
"""
from dataclasses import asdict
import json
import math
import os
from pathlib import Path
import time
import unittest
import uuid
from unittest.mock import patch
from .bench import Bench, FEATURES, get_json, open_direct
from .store import Store
from .efficiency_pilot import perform, write_json
from .efficiency_schedule import Schedule, Action, post_schedule

ROOT = Path(__file__).resolve().parents[1]


class LiveScheduleTests(unittest.TestCase):
    def wait_sample(self, bench, after, expected_rate, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with bench.journal.db() as db:
                sample = db.execute('SELECT * FROM samples WHERE id>? ORDER BY id DESC LIMIT 1', (after,)).fetchone()
            if sample is not None:
                raw = json.loads(sample['counters'])
                if raw['client_target_rps'] == expected_rate:
                    return dict(sample)
            self.assertFalse(bench.stop_event.is_set(), 'Collector stopped unexpectedly.')
            time.sleep(.03)
        self.fail('No new real sample with the selected request rate.')

    def check_order(self, order):
        parent = ROOT / 'work/efficiency-live-check-v01'
        parent.mkdir(exist_ok=True)
        directory = parent / ('order-' + str(order) + '-' + uuid.uuid4().hex[:12])
        directory.mkdir()
        fixture = {'scope': 'Technical live-child check, not research data.', 'order': order,
                   'artificial_bootstrap_rows': 896, 'actual_normal_training_collected': False,
                   'real_schedule_deadlines_waited': False, 'real_commands_and_http_requests': True,
                   'manual_probe_requests_added': True, 'quality_evaluated': False,
                   'child_pid': None, 'events': [], 'status': 'starting'}
        write_json(directory / 'technical_fixture.json', fixture)
        store = Store(directory / 'monitor.sqlite3')
        bench = Bench(store)
        original_register = store.register

        def register_with_artificial_bootstrap(data):
            result = original_register(data)
            # Test-only, legitimate readiness through the actual Store/Detector.
            # No production readiness flag or control gate is patched.
            rows = [{'timestamp': i + 1, 'values': dict(zip(FEATURES,
                     (2 + math.sin(i * .1), 30 + .1 * math.sin(i * .17),
                      .5 + .4 * math.sin(i * .13), 2 + .2 * math.cos(i * .11))))}
                    for i in range(896)]
            for start in range(0, 896, 128):
                store.ingest(data['id'], {'observations': rows[start:start + 128]})
            return result

        process = None
        try:
            with patch.object(store, 'register', side_effect=register_with_artificial_bootstrap):
                run = bench.start()
            process = bench.process
            fixture.update(run=run['id'], child_pid=process.pid)
            self.assertNotEqual(process.pid, os.getpid())
            self.assertIsNone(process.poll())
            state = store.state(run['host'])[0]
            self.assertEqual(state['samples'], 896)
            self.assertEqual(state['phase'], 'monitoring')
            scheduler = Schedule(order)
            last_sample = 0
            for expected in post_schedule(order):
                state = store.state(run['host'])[0]
                actions = scheduler.tick(state['samples'], state['phase'], expected.at)
                self.assertEqual(actions, [expected])
                started = time.time()
                result = perform(bench, actions[0])
                entry = {'action': asdict(expected), 'real_requested': started,
                         'real_completed': time.time(), 'result': result}
                if expected.kind == 'stop':
                    fixture['events'].append(entry)
                    break
                rate = expected.value if expected.kind == 'load' else 2
                self.assertEqual(bench.effective_load(), rate)
                before = get_json(bench.url + '/metrics', bench.token)
                bench.one_request(run['id'])
                sample = self.wait_sample(bench, last_sample, rate)
                last_sample = sample['id']
                self.assertEqual(set(json.loads(sample['detector_values'])), set(FEATURES))
                after = get_json(bench.url + '/metrics', bench.token)
                self.assertGreater(after['requests_completed'], before['requests_completed'])
                with bench.journal.db() as db:
                    observed = [dict(r) for r in db.execute(
                        'SELECT r.*,w.cpu_ms,w.iterations FROM requests r LEFT JOIN request_work w '
                        'ON w.request_id=r.id WHERE r.run=? AND r.sent>=? ORDER BY r.id', (run['id'], started))]
                self.assertTrue(observed)
                self.assertTrue(all(r['status'] == 200 and r['error'] is None for r in observed))
                if expected.kind == 'extra_cpu':
                    executed = [r for r in observed if r['served_mode'] == 'extra_cpu' and r['iterations'] and r['iterations'] > 0]
                    self.assertTrue(executed)
                    self.assertGreater(after['extra_work_iterations'], before['extra_work_iterations'])
                    self.assertGreaterEqual(sum(r['cpu_ms'] for r in executed), 0)
                    with self.assertRaises(ValueError):
                        bench.set_load(8, 1)
                    entry['executed_extra_requests'] = len(executed)
                    entry['measured_extra_cpu_ms'] = sum(r['cpu_ms'] for r in executed)
                else:
                    self.assertTrue(all(r['served_mode'] == 'normal' for r in observed))
                    if rate > 2:
                        with self.assertRaises(ValueError):
                            bench.control('extra_cpu', 1)
                entry['new_real_sample_id'] = last_sample
                entry['real_requests_observed'] = len(observed)
                fixture['events'].append(entry)
                write_json(directory / 'technical_fixture.json', fixture)
                # Separate real-time expiry probe after returning from second
                # extra-work event. Does not pretend to verify 10/30s duration.
                if expected.at == post_schedule(order)[-2].at:
                    perform(bench, Action(0, 'extra_cpu', 0, 1))
                    bench.one_request(run['id'])
                    time.sleep(1.1)
                    with open_direct(bench.url + '/work') as response:
                        response.read()
                        self.assertEqual(response.headers['X-Workbench-Mode'], 'normal')
                    self.assertEqual(bench.state()['current']['mode'], 'normal')
                    fixture['real_one_second_expiry_checked'] = True
            self.assertTrue(scheduler.finished)
            bench.thread.join(timeout=12)
            self.assertFalse(bench.thread.is_alive())
            self.assertIsNotNone(process.poll())
            with bench.journal.db() as db:
                counts = tuple(db.execute('SELECT (SELECT count(*) FROM requests),'
                                          '(SELECT count(*) FROM samples)').fetchone())
                status = db.execute('SELECT status,error FROM runs WHERE id=?', (run['id'],)).fetchone()
                self.assertEqual(status['status'], 'stopped')
                self.assertIsNone(status['error'])
            time.sleep(.15)
            with bench.journal.db() as db:
                self.assertEqual(counts, tuple(db.execute('SELECT (SELECT count(*) FROM requests),'
                                                        '(SELECT count(*) FROM samples)').fetchone()))
            fixture.update(status='passed', real_sample_rows=counts[1], real_request_rows=counts[0],
                           own_child_exited=True, collector_thread_exited=True,
                           records_stable_after_stop=True)
        except BaseException as exc:
            fixture.update(status='failed', error=type(exc).__name__ + ': ' + str(exc))
            raise
        finally:
            bench.close()
            fixture['own_child_exited_after_cleanup'] = process is None or process.poll() is not None
            write_json(directory / 'technical_fixture.json', fixture)

    def test_real_child_order_1(self):
        self.check_order(1)

    def test_real_child_order_2(self):
        self.check_order(2)


if __name__ == '__main__':
    unittest.main(verbosity=2)
