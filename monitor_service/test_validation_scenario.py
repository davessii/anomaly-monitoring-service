"""Virtual-clock/API software fixtures. Never start the five real recordings."""
import copy
from contextlib import closing
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from .validation_scenario import (ROOT, Api, Evidence, Scenario, load_protocol,
                                  main, save, schedule, sha, verify_bundle)


class FakeClock:
    def __init__(self):
        self.now = 0.
        self.wall_jump = 0.

    def stamp(self):
        return dict(utc=1800000000 + self.now + self.wall_jump, monotonic=self.now)

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class FakeApi:
    def __init__(self, clock, protocol):
        self.clock, self.protocol = clock, protocol
        self.calls = []
        self.app = self.reader = False
        self.slow_command = False
        self.unfit = False
        self.other_app = False
        self.warmup_never_ready = False

    def call(self, path, body=None):
        self.calls.append((path, copy.deepcopy(body), self.clock.now))
        if body is not None:
            self.clock.sleep(1.2 if self.slow_command and path == '/api/bench/load' else .02)
        if path == '/api/health':
            return dict(version='0.7.0', precise_log_quality=True)
        if path == '/api/state':
            return dict(jobs=[])
        if path == '/api/bench/start':
            self.app = True
            return dict(id='app-fixture', running=True, started_at=self.clock.stamp()['utc'],
                        access_log_file='app-fixture-access.log',
                        access_log_profile='owned_access_us_v2')
        if path == '/api/logs/start':
            self.reader = True
            return dict(id='log-fixture', host='log-fixture')
        if path == '/api/bench':
            active = self.app or self.other_app
            current = dict(id='app-other' if self.other_app else 'app-fixture', running=active)
            return dict(current=current if active or self.app else None,
                        runs=[dict(id='app-fixture', monitor=dict(phase='monitoring', samples=896))] if self.app else [])
        if path == '/api/logs':
            return dict(sources=[] if not self.app and not self.reader else [dict(
                source='log-fixture', config=copy.deepcopy(self.protocol['source_config']),
                status='active', worker=dict(running=self.reader, error=None),
                model=dict(phase='unfit' if self.unfit else 'monitoring',
                           ready=not self.unfit and not self.warmup_never_ready, samples=896))])
        if path in ('/api/bench/control', '/api/bench/load'):
            return dict(ok=True)
        if path == '/api/logs/stop':
            self.reader = False
            return dict(running=False)
        if path == '/api/bench/stop':
            self.app = False
            return dict(stopping=True)
        raise AssertionError('Unexpected API call: ' + path)


class FakeEvidence:
    monitor = application = Path('not-a-real-database')

    def connect(self, path):
        return io.StringIO()

    def check_model(self, host, protocol):
        return dict(fixture=True, ready=True, config=copy.deepcopy(protocol['model_config']))

    def runtime(self, run, protocol):
        return dict(fixture=True, **protocol['runtime_versions'])

    def export(self, output, run, source, filename):
        save(output / 'fixture-only.json', dict(real_requests=0, fixture=True))


class ScenarioTests(unittest.TestCase):
    def setUp(self):
        self.protocol = load_protocol()
        root = ROOT / 'work/validation-scenario-unit-tests'
        root.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=root)
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        self.clock = FakeClock()
        self.api = FakeApi(self.clock, self.protocol)

    def runner(self, number=1, gate=lambda: 'test-seal'):
        return Scenario(self.protocol, number, self.api, FakeEvidence(), self.folder / 'recording',
                        clock=self.clock, check_bundle=gate)

    def test_all_five_schedules_are_fixed_before_any_data(self):
        self.assertEqual([r['normal'] + r['load'] for r in self.protocol['runs']], [50, 57, 64, 71, 78])
        self.assertEqual([r['delay'] for r in self.protocol['runs']], [15, 12, 18, 14, 16])
        for number in range(1, 6):
            plan = schedule(self.protocol, number)
            self.assertEqual([s['stage'] for s in plan], ['normal', 'load', 'delay', 'errors', 'recovery'])
            self.assertEqual(plan[1]['duration_seconds'], 20)
            self.assertEqual(plan[-1]['duration_seconds'], 20)
        self.assertEqual(schedule(self.protocol, 1)[-1]['end_seconds'], 100)

    def test_default_command_only_describes_and_never_constructs_api(self):
        with patch('monitor_service.validation_scenario.verify_bundle', return_value='fixture'), \
                patch('monitor_service.validation_scenario.Api') as api, patch('sys.stdout', new_callable=io.StringIO) as output:
            self.assertEqual(main(['--run', '1']), 0)
        api.assert_not_called()
        self.assertEqual(json.loads(output.getvalue())['real_recordings_started'], 0)

    def test_execute_requires_explicit_run_number(self):
        with patch('sys.stderr', new_callable=io.StringIO), self.assertRaises(SystemExit):
            main(['--execute'])
        self.assertEqual(list(self.folder.iterdir()), [])

    def test_full_virtual_schedule_uses_real_commands_not_fabricated_log_lines(self):
        runner = self.runner()
        result = runner.execute()
        self.assertEqual(result['status'], 'recorded_not_evaluated')
        self.assertFalse(result['quality_evaluated'])
        self.assertFalse(self.api.app or self.api.reader)
        self.assertAlmostEqual(result['finished']['monotonic'] - result['origin']['monotonic'], 104.04)
        self.assertEqual([b['start_seconds'] for b in result['boundaries']], [0, 30, 50, 65, 80])
        controls = [body for path, body, _ in self.api.calls if path == '/api/bench/control']
        self.assertEqual(controls, [dict(mode='normal', seconds=30), dict(mode='delay', seconds=15),
                                   dict(mode='error', seconds=15), dict(mode='normal', seconds=20)])
        self.assertEqual([body['rate'] for path, body, _ in self.api.calls if path == '/api/bench/load'], [8, 2])
        self.assertFalse(any(path.endswith('/settings') for path, _, _ in self.api.calls))
        self.assertFalse((runner.output / 'validation_results.json').exists())
        events = [json.loads(line) for line in (runner.output / 'scenario_events.jsonl').read_text(encoding='utf-8').splitlines()]
        self.assertEqual(sum(e['kind'] == 'phase_begin' for e in events), 5)
        self.assertTrue(all('sent' in c and 'acknowledged' in c for c in result['commands']))

    def test_late_boundary_is_retained_as_invalid_and_never_retried(self):
        self.api.slow_command = True
        runner = self.runner()
        result = runner.execute()
        self.assertEqual(result['status'], 'invalid')
        self.assertIn('одну секунду', result['error'])
        self.assertFalse(any(body and body.get('mode') == 'delay' for _, body, _ in self.api.calls))
        self.assertFalse(self.api.app or self.api.reader)
        self.assertTrue((runner.output / 'run_manifest.json').exists())

    def test_existing_attempt_is_never_overwritten_or_started_again(self):
        runner = self.runner()
        runner.output.mkdir()
        (runner.output / 'keep.txt').write_text('previous failed attempt')
        with self.assertRaises(FileExistsError):
            runner.execute()
        self.assertEqual(self.api.calls, [])
        self.assertEqual((runner.output / 'keep.txt').read_text(), 'previous failed attempt')

    def test_other_application_is_not_stopped_or_reused(self):
        self.api.other_app = True
        result = self.runner().execute()
        self.assertEqual(result['status'], 'invalid')
        self.assertFalse(any(path.endswith('/start') or path.endswith('/stop') for path, _, _ in self.api.calls))
        self.assertTrue(self.api.other_app)

    def test_unfit_warmup_stops_owned_processes_without_entering_scenario(self):
        self.api.unfit = True
        result = self.runner().execute()
        self.assertEqual(result['status'], 'invalid')
        self.assertFalse(self.api.app or self.api.reader)
        self.assertFalse(any(path == '/api/bench/control' for path, _, _ in self.api.calls))

    def test_warmup_timeout_has_no_shortcut_or_silent_reset(self):
        self.api.warmup_never_ready = True
        result = self.runner().execute()
        self.assertEqual(result['status'], 'invalid')
        self.assertIn('1200', result['error'])
        self.assertEqual(sum(path == '/api/bench/start' for path, _, _ in self.api.calls), 1)

    def test_new_seal_mid_recording_cannot_replace_initial_authorization(self):
        count = 0
        def gate():
            nonlocal count
            count += 1
            return 'first-seal' if count == 1 else 'different-seal'
        result = self.runner(gate=gate).execute()
        self.assertEqual(result['status'], 'invalid')
        self.assertIn('заменена', result['error'])

    def test_clock_jump_is_not_reported_as_precise_latency(self):
        runner = self.runner()
        old = runner.apply_phase
        def jump(item):
            result = old(item)
            if item['stage'] == 'load':
                self.clock.wall_jump = 3.
            return result
        runner.apply_phase = jump
        result = runner.execute()
        self.assertEqual(result['status'], 'invalid')
        self.assertIn('часы', result['error'])

    def test_hash_gate_rejects_modified_file(self):
        target = self.folder / 'pinned.txt'
        target.write_text('unchanged', encoding='utf-8')
        save(self.folder / 'seal.json', dict(id='service-validation-v1', version='1.0.0',
             files={target.relative_to(ROOT).as_posix(): sha(target)}))
        verify_bundle(self.folder)
        target.write_text('edited', encoding='utf-8')
        with self.assertRaises(ValueError):
            verify_bundle(self.folder)

    def test_external_targets_and_credentials_are_rejected_before_requests(self):
        for url in ('https://example.com', 'http://example.com:8765', 'http://user:pass@127.0.0.1:8765',
                    'http://127.0.0.1:8765/another/path'):
            with self.assertRaises(ValueError):
                Api(url, 3)

    def test_evidence_exports_all_rows_for_only_one_run_read_only(self):
        evidence = Evidence(self.folder / 'data')
        evidence.application.parent.mkdir(parents=True)
        source, run = 'log-fixture', 'app-fixture'
        access = self.folder / (run + '-access.log')
        access.write_bytes(b'v2 ts=1800000000.100 status=200 rt=0.000408\n')
        state = dict(ready=True, config=self.protocol['model_config'],
                     lengths=[256, 128, 512], features=self.protocol['source_config']['features'],
                     log_settings=self.protocol['source_config']['log_settings'], processing_mode='log_quality_v2')
        with closing(sqlite3.connect(evidence.monitor)) as db, db:
            db.executescript('CREATE TABLE hosts(id,state); CREATE TABLE log_sources(id,host,path); '
                             'CREATE TABLE observations(host,ts,payload); CREATE TABLE log_messages(host,ts,details); '
                             'CREATE TABLE log_windows(source,start_ms,payload)')
            db.execute('INSERT INTO hosts VALUES(?,?)', (source, json.dumps(state)))
            db.execute('INSERT INTO log_sources VALUES(?,?,?)', (source, source, str(access)))
            for table, key in (('observations', source), ('log_messages', source), ('log_windows', source)):
                db.executemany('INSERT INTO ' + table + ' VALUES(?,?,?)', [(key, i, '{}') for i in range(600)] + [('unrelated', 0, '{}')])
        with closing(sqlite3.connect(evidence.application)) as db, db:
            for name in ('requests', 'controls', 'load_changes'):
                db.execute('CREATE TABLE ' + name + '(run,id,value)')
                db.executemany('INSERT INTO ' + name + ' VALUES(?,?,?)', [(run, i, 'fixture') for i in range(600)] + [('unrelated', 0, 'keep')])
        baseline = sha(evidence.monitor), sha(evidence.application)
        self.assertTrue(evidence.check_model(source, self.protocol)['ready'])
        output = self.folder / 'export'
        output.mkdir()
        evidence.export(output, run, source, access.name)
        for name in ('observations', 'messages', 'windows', 'requests', 'controls', 'load_changes'):
            self.assertEqual(len((output / (name + '.jsonl')).read_text(encoding='utf-8').splitlines()), 600)
        self.assertEqual(baseline, (sha(evidence.monitor), sha(evidence.application)))
        self.assertEqual((output / 'access.log').read_bytes(), access.read_bytes())
        changed = copy.deepcopy(self.protocol)
        changed['model_config']['forgetting'] = .5
        with self.assertRaises(ValueError):
            evidence.check_model(source, changed)


if __name__ == '__main__':
    unittest.main(verbosity=2)
