"""Loopback HTTP + worker tests in isolated databases, no research collection."""
from html.parser import HTMLParser
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import urllib.error
import urllib.request

from .store import Store, encode, Conflict
from .server import Server
from .log_worker import LogWorkers
from .log_ingest import digest
from .log_schema import FEATURES
from .immediate import MODE, ImmediateDetector
from .test_log_tailer import BASE, line

ROOT = Path(__file__).resolve().parents[1]


class Clock:
    def __init__(self):
        self.value = BASE
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            return self.value

    def set(self, value):
        with self.lock:
            self.value = value


class LogWorkerTests(unittest.TestCase):
    def setUp(self):
        self.root = ROOT/'work/log-worker-test-tmp'
        self.root.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix='case-', dir=self.root)
        self.addCleanup(self.cleanup_directory)
        self.data = Path(self.temp.name)
        self.db_path = self.data/'monitor.sqlite3'
        self.folder = self.data/'log_inputs'
        self.folder.mkdir()
        self.path = self.folder/'access.log'
        self.path.write_bytes(b'')
        self.clock = Clock()
        self.server = None

    def cleanup_directory(self):
        self.stop_server()
        target, root = Path(self.temp.name).resolve(), self.root.resolve()
        if target == root or not target.is_relative_to(root):
            raise RuntimeError('Test cleanup escaped workspace')
        self.temp.cleanup()

    def start_server(self):
        self.store = Store(self.db_path)
        self.server = Server(('127.0.0.1', 0), self.store, self.folder)
        self.server.logs.clock_ms = self.clock
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}'

    def stop_server(self):
        if self.server is not None:
            self.server.close()
            self.thread.join(timeout=3)
            self.server = None

    def request(self, path, data=None, headers=None):
        request = urllib.request.Request(self.url+path,
            data=None if data is None else encode(data).encode('utf-8'),
            headers=headers or ({'Content-Type': 'application/json'} if data is not None else {}))
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.read()

    def api(self, path='/api/logs', data=None):
        return json.loads(self.request(path, data))

    def wait(self, predicate):
        deadline = time.monotonic()+5
        while time.monotonic() < deadline:
            state = self.api()
            if predicate(state):
                return state
            time.sleep(.025)
        self.fail('Worker did not reach expected state: '+encode(self.api()))

    def create(self):
        result = self.api('/api/logs/start', {'filename': 'access.log'})
        self.wait(lambda s: len(s['sources']) == 1 and s['sources'][0]['worker']['last_poll_ms'] is not None)
        return result['id']

    def append(self, value):
        with self.path.open('ab', buffering=0) as stream:
            stream.write(value)

    def test_tick_samples_clock_after_read_not_only_before_poll(self):
        store = Store(self.db_path)
        store.register(dict(id='web', source='external', features=list(FEATURES),
                            settings=dict(components=1, processing_mode=MODE)))
        workers = LogWorkers(store, self.folder)
        self.addCleanup(workers.close)
        workers.ingest.attach('access', 'web', self.path, start_ms=BASE)
        calls = []
        def concurrent_clock():
            if not calls:
                # Simulate the writer appending after the first time sample.
                self.append(line(101))
                result = BASE+100
            else:
                result = BASE+102
            calls.append(result)
            return result
        workers.clock_ms = concurrent_clock
        workers.tick('access')
        self.assertEqual(calls, [BASE+100, BASE+102, BASE+102])
        state = workers.ingest.status('access')
        self.assertEqual(state['status'], 'active')
        self.assertEqual(state['aggregate']['accepted_records'], 1)
        self.assertEqual(state['reader']['last_clock_ms'], BASE+102)

    def test_http_inventory_only_exposes_allowed_log_files(self):
        (self.folder/'notes.txt').write_text('not a log', encoding='utf-8')
        (self.folder/'nested').mkdir()
        (self.folder/'nested/hidden.log').write_bytes(b'')
        self.start_server()
        state = self.api()
        self.assertEqual([item['name'] for item in state['files']], ['access.log'])
        self.assertEqual(state['directory'], str(self.folder.resolve()))
        self.assertEqual(state['sources'], [])

    def test_no_browser_path_or_custom_profile_can_be_submitted(self):
        self.start_server()
        for data in ({'filename': '../outside.log'}, {'filename': str(self.path)},
                     {'filename': 'nested/hidden.log'}, {'filename': 'nested\\hidden.log'},
                     {'filename': 'access.log:secret'}, {'filename': 'notes.txt'},
                     {'path': str(self.path)}, {'filename': 'access.log', 'mode': 'replay'},
                     {'filename': 'access.log', 'source_id': 'anything'}):
            with self.subTest(data=data), self.assertRaises(urllib.error.HTTPError) as caught:
                self.api('/api/logs/start', data)
            self.assertEqual(caught.exception.code, 400)
        self.assertEqual(self.api('/api/state')['hosts'], [])

    def test_symlink_flag_is_rejected_before_opening_file(self):
        manager = LogWorkers(Store(self.db_path), self.folder, clock_ms=self.clock)
        try:
            with patch.object(Path, 'is_symlink', return_value=True), self.assertRaises(ValueError):
                manager._file('access.log')
        finally:
            manager.close()

    def test_start_and_stop_are_posts_and_cross_site_requests_are_rejected(self):
        self.start_server()
        for path in ('/api/logs/start', '/api/logs/stop'):
            with self.assertRaises(urllib.error.HTTPError) as caught:
                self.request(path)
            self.assertEqual(caught.exception.code, 404)
        with self.assertRaises(urllib.error.HTTPError) as caught:
            self.request('/api/logs/start', {'filename': 'access.log'},
                         {'Content-Type': 'application/json', 'Origin': 'https://foreign.invalid'})
        self.assertEqual(caught.exception.code, 403)
        self.assertEqual(self.api('/api/state')['hosts'], [])

    def test_new_source_is_fresh_external_immediate_model_with_fixed_profile(self):
        self.start_server()
        source = self.create()
        state = self.api('/api/state')
        host, = state['hosts']
        self.assertEqual((host['id'], host['source'], host['features'], host['components']),
                         (source, 'external', list(FEATURES), 1))
        self.assertEqual((host['warmup_required'], host['samples'], host['delivery_rows']), (896, 0, 1))
        self.assertEqual(state['log_sources'][0]['source'], source)

    def test_double_start_keeps_one_worker_and_does_not_register_second_model(self):
        self.start_server()
        source = self.create()
        repeated = self.api('/api/logs/start', {'source_id': source})
        self.assertTrue(repeated['already_running'])
        with self.assertRaises(urllib.error.HTTPError) as caught:
            self.api('/api/logs/start', {'filename': 'access.log'})
        self.assertEqual(caught.exception.code, 409)
        self.assertEqual(len(self.api('/api/state')['hosts']), 1)

    def test_stop_preserves_partial_bytes_and_restart_resumes_them(self):
        self.start_server()
        source = self.create()
        generation = self.api()['sources'][0]['generation']
        value = line(1100)
        self.append(value[:15])
        self.clock.set(BASE+1150)
        self.wait(lambda s: s['sources'][0]['reader']['buffered_bytes'] == 15)
        self.api('/api/logs/stop', {'source_id': source})
        stopped = self.api()['sources'][0]
        self.assertFalse(stopped['worker']['running'])
        self.append(value[15:])
        self.clock.set(BASE+1200)
        self.api('/api/logs/start', {'source_id': source})
        self.wait(lambda s: s['sources'][0]['aggregate']['accepted_records'] == 1)
        self.clock.set(BASE+4000)
        final = self.wait(lambda s: s['sources'][0]['model']['samples'] == 1)['sources'][0]
        self.assertEqual(final['generation'], generation)
        self.assertEqual(final['latest_window']['payload']['request_count'], 1)

    def test_server_restart_does_not_auto_start_or_reset_saved_observation(self):
        self.start_server()
        source = self.create()
        self.append(line(1100, 503, 60))
        self.clock.set(BASE+1200)
        self.wait(lambda s: s['sources'][0]['aggregate']['accepted_records'] == 1)
        self.clock.set(BASE+4000)
        first = self.wait(lambda s: s['sources'][0]['model']['samples'] == 1)['sources'][0]
        self.stop_server()
        self.start_server()
        restored, = self.api()['sources']
        self.assertFalse(restored['worker']['running'])
        self.assertEqual(restored['generation'], first['generation'])
        self.assertEqual(restored['model']['samples'], 1)
        self.api('/api/logs/start', {'source_id': source})
        self.wait(lambda s: s['sources'][0]['worker']['last_poll_ms'] is not None)
        self.assertEqual(self.api()['sources'][0]['model']['samples'], 1)

    def test_empty_interval_pauses_model_but_shows_missing_data(self):
        self.start_server()
        self.create()
        self.clock.set(BASE+4000)
        source = self.wait(lambda s: s['sources'][0]['status'] == 'degraded')['sources'][0]
        self.assertEqual(source['latest_window']['payload']['quality'], 'empty')
        self.assertIsNone(source['latest_window']['payload']['metrics']['mean_request_ms'])
        self.assertEqual(source['model']['samples'], 0)
        self.assertIn('приостановлена', source['reason_text'])

    def test_missing_file_stops_worker_and_persists_block_reason(self):
        self.start_server()
        self.create()
        self.path.unlink()
        source = self.wait(lambda s: not s['sources'][0]['worker']['running'])['sources'][0]
        self.assertEqual(source['status'], 'needs_restart')
        self.assertEqual(source['reason'], 'file_missing')
        self.assertIn('исчез', source['reason_text'])

    def test_replaced_file_cannot_be_resumed_without_explicit_new_observation(self):
        self.start_server()
        source = self.create()
        replacement = self.folder/'replacement.tmp'
        replacement.write_bytes(b'')
        replacement.replace(self.path)
        self.wait(lambda s: s['sources'][0]['status'] == 'needs_restart' and not s['sources'][0]['worker']['running'])
        with self.assertRaises(urllib.error.HTTPError) as caught:
            self.api('/api/logs/start', {'source_id': source})
        self.assertEqual(caught.exception.code, 409)
        new = self.api('/api/logs/start', {'filename': 'access.log'})
        self.assertNotEqual(new['id'], source)
        self.assertEqual(len(self.api('/api/state')['hosts']), 2)

    def test_failed_attach_does_not_leave_an_unused_model(self):
        manager = LogWorkers(Store(self.db_path), self.folder, clock_ms=self.clock)
        try:
            with patch.object(manager.ingest, 'attach', side_effect=RuntimeError('fixture failure')):
                with self.assertRaises(RuntimeError):
                    manager.start({'filename': 'access.log'})
            self.assertEqual(manager.store.state(), [])
        finally:
            manager.close()

    def test_closed_manager_will_not_start_a_worker(self):
        manager = LogWorkers(Store(self.db_path), self.folder, clock_ms=self.clock)
        manager.close()
        with self.assertRaises(Conflict):
            manager.start({'filename': 'access.log'})

    def test_maximum_workers_is_checked_before_creating_model(self):
        manager = LogWorkers(Store(self.db_path), self.folder, clock_ms=self.clock)
        try:
            with patch('monitor_service.log_worker.MAX_WORKERS', 0):
                with self.assertRaises(Conflict):
                    manager.start({'filename': 'access.log'})
            self.assertEqual(manager.store.state(), [])
        finally:
            manager.close()

    def test_real_http_worker_reaches_decision_using_prepared_ready_fixture(self):
        self.start_server()
        source = self.create()
        self.api('/api/logs/stop', {'source_id': source})
        # Prepare a ready model solely for this integration fixture. Full actual
        # 896-row warmup/delivery equivalence is separately tested in test_log_ingest.
        model = ImmediateDetector(FEATURES, components=1)
        for i in range(896):
            model.observe(BASE/1000-896+i, [1.+i % 4, 20.+i*7 % 101, (i % 13 == 0)*.25])
        snapshot = encode(model.to_state())
        with self.store.connection() as db:
            db.execute('UPDATE hosts SET state=?,last_ts=? WHERE id=?', (snapshot, BASE/1000-1, source))
            db.execute('UPDATE log_sources SET host_state_sha256=? WHERE id=?', (digest(snapshot), source))
        self.append(line(1100, 200, 40)+line(1900, 503, 60))
        self.clock.set(BASE+1950)
        self.api('/api/logs/start', {'source_id': source})
        self.wait(lambda s: s['sources'][0]['aggregate']['accepted_records'] == 2)
        self.clock.set(BASE+4000)
        state = self.wait(lambda s: s['sources'][0]['model']['samples'] == 897)['sources'][0]
        history = self.api(f'/api/hosts/{source}/history')
        self.assertEqual(len(history['observations']), 1)
        self.assertIsNotNone(history['observations'][0]['decision'])
        self.assertEqual(history['observations'][0]['values'], dict(zip(FEATURES, [2., 50., .5])))
        self.assertEqual(state['latest_window']['payload']['error_5xx_count'], 1)
        self.api('/api/logs/stop', {'source_id': source})
        self.api('/api/logs/start', {'source_id': source})
        self.wait(lambda s: s['sources'][0]['worker']['last_poll_ms'] is not None)
        self.assertEqual(self.api()['sources'][0]['model']['samples'], 897)

    def test_ui_assets_have_unique_ids_and_plain_log_controls(self):
        self.start_server()
        page = self.request('/').decode('utf-8')

        class Ids(HTMLParser):
            def __init__(self):
                super().__init__(); self.values = []
            def handle_starttag(self, tag, attrs):
                self.values.extend(value for key, value in attrs if key == 'id')

        ids = Ids(); ids.feed(page)
        self.assertEqual(len(ids.values), len(set(ids.values)))
        for name in ('log-create', 'log-resume', 'log-stop', 'log-state', 'log-latest'):
            self.assertIn(name, ids.values)
        self.assertIn('Новое наблюдение', page)
        self.assertIn('Наблюдение за сервером', page)
        self.assertIn('/api/logs/start', self.request('/logs.js').decode('utf-8'))
        self.assertIn('explainLogState', self.request('/app.js').decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
