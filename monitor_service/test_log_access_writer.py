"""Owned writer/HTTP fixtures; no experimental quality comparisons."""
from concurrent.futures import ThreadPoolExecutor
from http.client import RemoteDisconnected
from pathlib import Path
import tempfile
import threading
import time
import unittest
import urllib.error
from unittest.mock import patch

from .log_access_writer import AccessLogWriter, format_line
from .log_parser import parse_line, LogParseError
from .test_app import Application, ApplicationHandler
from .bench import Bench, get_json, command, open_direct
from .store import Store
from .server import Server
from .test_log_tailer import BASE

ROOT = Path(__file__).resolve().parents[1]


class AccessWriterTests(unittest.TestCase):
    def setUp(self):
        self.root = ROOT/'work/access-writer-test-tmp'
        self.root.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix='case-', dir=self.root)
        self.addCleanup(self.cleanup_directory)
        self.folder = Path(self.temp.name)
        self.path = self.folder/'access.log'
        self.app = None
        self.server = None

    def cleanup_directory(self):
        if self.server is not None:
            self.server.close();self.server_thread.join(timeout=5)
        if self.app is not None:
            self.app.close();self.app_thread.join(timeout=5)
        target, root = Path(self.temp.name).resolve(), self.root.resolve()
        if target == root or not target.is_relative_to(root):
            raise RuntimeError('Test cleanup escaped workspace')
        self.temp.cleanup()

    def launch(self):
        self.app = Application(access_log=self.path)
        self.app_thread = threading.Thread(target=self.app.serve_forever, daemon=True)
        self.app_thread.start()
        self.url = f'http://127.0.0.1:{self.app.server_port}'

    def records(self, count):
        deadline = time.monotonic()+3
        while time.monotonic() < deadline:
            values = self.path.read_bytes().splitlines(keepends=True)
            if len(values) >= count:
                return values
            time.sleep(.01)
        self.fail('Access writer did not produce expected completed lines')

    def test_format_exact_milliseconds_without_float_rounding(self):
        self.assertEqual(format_line(BASE+123, 503, 42_999_999),
                         b'v1 ts=1792490400.123 status=503 rt=0.042\n')
        row = parse_line(format_line(BASE+999, 200, 0))
        self.assertEqual((row.timestamp_ms, row.status, row.request_time_ms), (BASE+999, 200, 0))

    def test_bad_ranges_and_types_are_rejected(self):
        for args in ((BASE, 99, 1), (BASE, 600, 1), (BASE, True, 1),
                     (BASE, 200, -1), (BASE, 200, 1.2), (-1, 200, 1),
                     (BASE, 200, 1_000_000_000_000_000_001)):
            with self.subTest(args=args), self.assertRaises(ValueError):
                format_line(*args)

    def test_unknown_status_is_explicit_000_and_parser_rejects_it(self):
        writer = AccessLogWriter(self.path, clock_ms=lambda: BASE+100)
        self.addCleanup(writer.close)
        self.assertTrue(writer.record(0, 10_000_000))
        self.assertEqual(writer.diagnostics()['unknown_status_records'], 1)
        with self.assertRaises(LogParseError):
            parse_line(self.path.read_bytes())
        self.assertIn(b'status=000', self.path.read_bytes())

    def test_existing_file_is_never_truncated_or_reused(self):
        self.path.write_bytes(b'previous run\n')
        with self.assertRaises(FileExistsError):
            AccessLogWriter(self.path)
        self.assertEqual(self.path.read_bytes(), b'previous run\n')

    def test_concurrent_requests_do_not_interleave_bytes(self):
        writer = AccessLogWriter(self.path, clock_ms=lambda: BASE+100)
        self.addCleanup(writer.close)
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda i: writer.record(200, (40+i)*1_000_000), range(32)))
        self.assertTrue(all(results))
        rows = [parse_line(value) for value in self.path.read_bytes().splitlines(keepends=True)]
        self.assertEqual(sorted(row.request_time_ms for row in rows), list(range(40, 72)))
        self.assertEqual(writer.diagnostics()['written_records'], 32)

    def test_partial_write_latches_error_and_does_not_report_success(self):
        writer = AccessLogWriter(self.path, clock_ms=lambda: BASE+100)
        with patch.object(writer.stream, 'write', return_value=1) as write:
            self.assertFalse(writer.record(200, 0))
            self.assertFalse(writer.record(200, 0))
            self.assertEqual(write.call_count, 1)
        self.assertEqual(writer.diagnostics()['error'], 'OSError')
        self.assertEqual(writer.diagnostics()['written_records'], 0)
        writer.close()

    def test_health_metrics_controls_and_unknown_paths_are_excluded(self):
        self.launch()
        get_json(self.url+'/health')
        info = get_json(self.url+'/metrics', self.app.token)
        command(self.url+'/control', self.app.token, {'mode': 'normal', 'seconds': 1})
        with self.assertRaises(urllib.error.HTTPError):
            get_json(self.url+'/missing')
        self.assertEqual(self.path.read_bytes(), b'')
        self.assertEqual(info['access_log']['profile'], 'nginx_access_v1')
        self.assertNotIn('token', self.path.read_text())

    def test_real_responses_write_200_503_and_measured_delay(self):
        self.launch()
        get_json(self.url+'/work')
        command(self.url+'/control', self.app.token, {'mode': 'error', 'seconds': 1})
        with self.assertRaises(urllib.error.HTTPError) as caught:
            get_json(self.url+'/work')
        self.assertEqual(caught.exception.code, 503)
        caught.exception.close()
        self.app.state.delay_seconds = .03
        command(self.url+'/control', self.app.token, {'mode': 'delay', 'seconds': 1})
        get_json(self.url+'/work')
        raw = self.records(3)
        rows = [parse_line(value) for value in raw]
        self.assertEqual([row.status for row in rows], [200, 503, 200])
        self.assertGreaterEqual(rows[-1].request_time_ms, 25)
        self.assertTrue(all(b'mode=' not in value and b'error' not in value and b'control' not in value for value in raw))

    def test_capacity_response_is_logged_as_429_not_server_error(self):
        self.launch()
        for _ in range(4):
            self.app.slots.acquire()
        try:
            with self.assertRaises(urllib.error.HTTPError) as caught:
                get_json(self.url+'/work')
            self.assertEqual(caught.exception.code, 429)
            caught.exception.close()
            self.assertEqual(parse_line(self.records(1)[0]).status, 429)
        finally:
            for _ in range(4):self.app.slots.release()

    def test_disconnected_response_is_not_written_as_200(self):
        self.launch()
        original = ApplicationHandler.send

        def broken_response(handler, *args, **kwargs):
            if handler.path == '/work':raise BrokenPipeError('software fixture')
            return original(handler, *args, **kwargs)

        with patch.object(ApplicationHandler, 'send', broken_response):
            with self.assertRaises((RemoteDisconnected, urllib.error.URLError)):
                get_json(self.url+'/work')
        self.assertIn(b'status=000', self.records(1)[0])

    def test_closing_application_drains_handlers_before_closing_log(self):
        self.launch()
        self.app.state.delay_seconds = .05
        self.app.state.set_mode('delay', 1)
        with ThreadPoolExecutor(max_workers=1) as pool:
            response = pool.submit(get_json, self.url+'/work')
            deadline = time.monotonic()+1
            while self.app.state.active == 0 and time.monotonic() < deadline:time.sleep(.001)
            self.app.close();self.app_thread.join(timeout=3)
            response.result(timeout=3)
        self.assertTrue(self.app.access_log.diagnostics()['closed'])
        self.assertEqual(parse_line(self.path.read_bytes()).status, 200)
        self.app = None

    def test_disabled_profile_has_no_writer_or_new_metrics(self):
        self.app = Application()
        self.app_thread = threading.Thread(target=self.app.serve_forever, daemon=True)
        self.app_thread.start()
        self.assertIsNone(self.app.access_log)
        self.assertNotIn('access_log', get_json(f'http://127.0.0.1:{self.app.server_port}/metrics'))

    def test_bench_detects_failed_writer_and_default_bench_stays_disabled(self):
        store = Store(self.folder/'monitor.sqlite3')
        bench = Bench(store)
        try:
            self.assertIsNone(bench.access_directory)
            bench.check_access_log({})
            bench.access_log_path = self.path
            for value in ({}, {'access_log': {'profile': 'wrong'}},
                          {'access_log': {'profile': 'nginx_access_v1', 'filename': self.path.name, 'error': 'OSError'}}):
                with self.assertRaises(ValueError):bench.check_access_log(value)
        finally:bench.close()

    def test_child_app_writes_real_file_consumed_by_service_and_keeps_client_journal(self):
        store = Store(self.folder/'monitor.sqlite3')
        self.server = Server(('127.0.0.1', 0), store, self.folder/'log_inputs')
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        base = f'http://127.0.0.1:{self.server.server_port}'

        def post(path, data):
            import json, urllib.request
            request = urllib.request.Request(base+path, data=json.dumps(data).encode(), headers={'Content-Type':'application/json'})
            with open_direct(request, timeout=15) as response:return json.load(response)

        run = post('/api/bench/start', {})
        access = self.folder/'log_inputs'/run['access_log_file']
        self.assertTrue(access.is_file())
        self.assertIn(access.name, [item['name'] for item in get_json(base+'/api/logs')['files']])
        connected = post('/api/logs/start', {'filename': access.name,'profile':'owned_access_us_v2','handler_limit_ms':10})
        deadline = time.monotonic()+10
        source = None
        while time.monotonic() < deadline:
            source = get_json(base+'/api/logs')['sources'][0]
            if source['model']['samples'] >= 1:break
            time.sleep(.05)
        self.assertGreaterEqual(source['model']['samples'], 1)
        self.assertEqual(source['status'], 'active')
        self.assertEqual(source['latest_window']['payload']['quality'], 'ok')
        self.assertNotEqual(connected['host'], run['host'])
        log_values = store.history(connected['host'])['observations'][-1]['values']
        self.assertEqual(set(log_values), set(('completed_rps','mean_request_ms','server_error_fraction')))
        self.assertEqual(log_values['server_error_fraction'], 0)
        self.assertEqual(get_json(base+'/api/bench')['access_log_monitor']['source'], connected['id'])
        self.server.guard_log_checks('another-file.log')  # No unrelated log model can block this run.
        with self.assertRaises(ValueError):self.server.guard_log_checks(access.name)
        post('/api/logs/stop', {'source_id': connected['id']})
        post('/api/bench/stop', {})
        deadline = time.monotonic()+10
        while self.server.bench.thread.is_alive() and time.monotonic() < deadline:time.sleep(.05)
        self.assertFalse(self.server.bench.thread.is_alive())
        from .log_precise import parse_precise_line
        rows = [parse_precise_line(value) for value in access.read_bytes().splitlines(keepends=True)]
        saved = get_json(base+'/api/bench')['runs'][0]
        self.assertGreater(len(rows), 0)
        self.assertEqual(len(rows), saved['request_count'])
        self.assertEqual(saved['config']['access_log']['profile'], 'owned_access_us_v2')
        self.assertTrue(all(row.status == 200 for row in rows))
        self.assertIsNotNone(self.server.bench.process.poll())


if __name__ == '__main__':unittest.main()
