"""Portable application checks; no historical experiments or private data."""
import json
from pathlib import Path
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from monitor_service.server import Server
from monitor_service.store import Store


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='monitor-service-test-')
        self.root = Path(self.temp.name).resolve()
        self.store = Store(self.root / 'monitor.sqlite3')
        self.server = Server(('127.0.0.1', 0), self.store)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = 'http://127.0.0.1:' + str(self.server.server_address[1])
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def tearDown(self):
        self.server.close()
        self.thread.join(timeout=5)
        self.assertFalse(self.thread.is_alive())
        self.assertEqual(Path(self.temp.name).resolve(), self.root)
        self.assertTrue(self.root.name.startswith('monitor-service-test-'))
        self.temp.cleanup()

    def request(self, path, body=None):
        data = None if body is None else json.dumps(body).encode('utf-8')
        request = urllib.request.Request(self.base + path, data=data,
            headers={} if body is None else {'Content-Type': 'application/json'})
        with self.opener.open(request, timeout=5) as response:
            return response.status, response.read()

    def register(self):
        return self.request('/api/hosts', {'id': 'sample', 'features': ['cpu', 'memory', 'requests']})

    def test_web_page_and_health(self):
        status, page = self.request('/')
        self.assertEqual(status, 200)
        self.assertIn(b'<html', page.lower())
        status, raw = self.request('/api/health')
        self.assertEqual(json.loads(raw)['status'], 'ok')

    def test_registration_ingest_and_persistence(self):
        self.assertEqual(self.register()[0], 201)
        self.request('/api/hosts/sample/observations', {'observations': [
            {'timestamp': 1, 'values': {'cpu': 12, 'memory': 40, 'requests': 5}}]})
        reopened = Store(self.store.path)
        self.assertEqual(reopened.state('sample')[0]['samples'], 1)
        self.assertEqual(len(reopened.history('sample', 10)['observations']), 1)

    def test_invalid_delivery_does_not_change_state(self):
        self.register()
        before = self.store.state('sample')
        with self.assertRaises(urllib.error.HTTPError) as caught:
            self.request('/api/hosts/sample/observations', {'observations': [
                {'timestamp': 1, 'values': {'cpu': 12, 'memory': 40, 'requests': None}}]})
        self.assertEqual(caught.exception.code, 400)
        self.assertEqual(before, self.store.state('sample'))

    def test_log_sources_initially_empty(self):
        status, raw = self.request('/api/state')
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(raw)['hosts'], [])

    def test_owned_application_starts_and_stops(self):
        current = self.server.bench.start()
        self.assertTrue(current['running'])
        with self.opener.open(self.server.bench.url + '/health', timeout=5) as response:
            self.assertEqual(json.load(response)['role'], 'owned-test-application')
        self.server.bench.stop()


if __name__ == '__main__':
    unittest.main()
