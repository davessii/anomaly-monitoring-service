"""Checks for worker ownership, fresh sockets, errors and durable collection."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout, contextmanager
from io import StringIO
import json
from pathlib import Path
import shutil
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import http.client
import urllib.request

from .cached_cycle_collection import WorkerOpeners, CachedCycleBench, collect, main
from .queued_cycle_collection import QueuedCycleBench, ResultWriter
from .store import Store
from .test_app import Application


@contextmanager
def workspace_fixture():
    root=(Path(__file__).resolve().parents[1]/'work').resolve()
    temporary=tempfile.TemporaryDirectory(dir=root,prefix='cached-v05-test-')
    target=Path(temporary.name).resolve()
    if not target.is_relative_to(root) or target==root:
        raise RuntimeError('Test cleanup target must stay below workspace work directory.')
    try:yield target
    finally:temporary.cleanup()


class ClientTests(unittest.TestCase):
    def setUp(self):
        self.app=Application(token='cached-client-fixture')
        self.thread=threading.Thread(target=self.app.serve_forever);self.thread.start()
        self.url='http://127.0.0.1:'+str(self.app.server_port)

    def tearDown(self):
        self.app.close();self.thread.join(3);self.assertFalse(self.thread.is_alive())

    def test_one_opener_but_fresh_connection_each_time_and_same_defaults(self):
        client=WorkerOpeners();connections=[];original=http.client.HTTPConnection.connect
        def connect(connection):
            original(connection);connections.append(connection)
        with patch.object(http.client.HTTPConnection,'connect',connect):
            for _ in range(2):
                with client.open(self.url+'/work') as response:
                    self.assertEqual(response.status,200);response.read()
                    self.assertEqual(response.headers['Connection'],'close')
        self.assertEqual(len(connections),2);self.assertIsNot(connections[0],connections[1])
        snapshot=client.snapshot();self.assertEqual(snapshot['opener_count'],1);self.assertEqual(snapshot['calls'],2)
        self.assertTrue(client.last_event()['cache_hit']);self.assertEqual(client.last_event()['build_ms'],0)
        opener,_=client.local.cached
        self.assertTrue(any(isinstance(h,urllib.request.HTTPSHandler) for h in opener.handlers))
        self.assertFalse(any(isinstance(h,urllib.request.ProxyHandler) for h in opener.handlers))

    def test_fields_match_old_collector_on_normal_response(self):
        old=object.__new__(QueuedCycleBench);old.url=self.url
        new=object.__new__(CachedCycleBench);new.url=self.url;new.http_client=WorkerOpeners()
        a=old.acquire_response('fixture');b=new.acquire_response('fixture')
        for key in ('run','status','error','mode','work','body_sha256'):
            self.assertEqual(a[key],b[key],key)

    def test_http_error_no_retry_and_next_normal_request_uses_same_opener(self):
        bench=object.__new__(CachedCycleBench);bench.url=self.url;bench.http_client=WorkerOpeners()
        self.app.state.set_mode('error',2)
        record=bench.acquire_response('fixture')
        self.assertEqual(record['error'],'HTTPError');self.assertIsNone(record['status'])
        self.assertEqual(self.app.state.started,1)
        self.app.state.set_mode('normal',2)
        self.assertEqual(bench.acquire_response('fixture')['status'],200)
        state=bench.http_client.snapshot()
        self.assertEqual(state['opener_count'],1);self.assertEqual(state['calls'],2);self.assertEqual(state['errors'],1)
        self.assertEqual(self.app.state.started,2)


class OwnershipTests(unittest.TestCase):
    def test_two_workers_do_not_share_openers(self):
        gate=threading.Barrier(2);constructed=[]
        def factory():
            owner=threading.get_ident();constructed.append(owner)
            class Opener:
                def open(self,request,timeout):
                    if threading.get_ident()!=owner:raise AssertionError('Cross-thread opener!')
                    return owner
            return Opener()
        client=WorkerOpeners(factory)
        def worker():
            gate.wait(2);a=client.open('fixture');b=client.open('fixture');return a,b
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures=[pool.submit(worker) for _ in range(2)]
            pairs=[f.result(timeout=3) for f in futures]
        self.assertEqual(len(set(constructed)),2);self.assertEqual(len(constructed),2)
        self.assertTrue(all(a==b for a,b in pairs));self.assertNotEqual(pairs[0][0],pairs[1][0])
        self.assertEqual(client.snapshot()['calls'],4)

    def test_timeout_no_automatic_retry_and_result_is_enqueued(self):
        calls=[];saved=[]
        class Opener:
            def open(self,request,timeout):
                calls.append(timeout);raise TimeoutError('Fixture timeout')
        bench=object.__new__(CachedCycleBench);bench.url='http://127.0.0.1:1'
        bench.http_client=WorkerOpeners(Opener);bench.writer=SimpleNamespace(submit=saved.append)
        bench.one_request('fixture');bench.one_request('fixture')
        self.assertEqual(calls,[2,2]);self.assertEqual(len(saved),2)
        self.assertTrue(all(r['error']=='TimeoutError' and r['status'] is None for r in saved))
        self.assertEqual(bench.http_client.snapshot()['opener_count'],1)

    def test_build_failure_does_not_retry(self):
        calls=[]
        def broken():calls.append(1);raise OSError('Fixture build error')
        client=WorkerOpeners(broken)
        with self.assertRaises(OSError):client.open('fixture')
        self.assertEqual(calls,[1]);self.assertEqual(client.snapshot()['failed_builds'],1)
        self.assertEqual(client.snapshot()['opener_count'],0)

    def test_slow_writer_does_not_hold_http_workers(self):
        from work.test_collector_diagnostic_v01 import Response
        release=threading.Event();entered=threading.Event();saved=[]
        def sink(record):entered.set();release.wait(3);saved.append(record)
        class Opener:
            def open(self,*args,**kwargs):return Response()
        bench=object.__new__(CachedCycleBench);bench.url='http://127.0.0.1:1'
        bench.http_client=WorkerOpeners(Opener)
        bench.writer=ResultWriter(sink,lambda error:None,8)
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures=[pool.submit(bench.one_request,'fixture') for _ in range(4)]
                for future in futures:future.result(timeout=1)
            self.assertTrue(entered.wait(1));self.assertEqual(bench.writer.accepted,4)
            self.assertEqual(len(saved),0)
        finally:release.set();bench.writer.finish()
        self.assertEqual(len(saved),4);self.assertLessEqual(bench.http_client.snapshot()['opener_count'],2)


class CollectionTests(unittest.TestCase):
    def test_owned_application_persistence_close_and_worker_accounting(self):
        with workspace_fixture() as folder:
            bench=CachedCycleBench(Store(Path(folder)/'monitor.sqlite3'))
            try:
                bench.start();bench.set_load(8,10);deadline=time.monotonic()+6
                while time.monotonic()<deadline and bench.writer.status()['persisted']<10:time.sleep(.05)
                self.assertGreaterEqual(bench.writer.status()['persisted'],10)
            finally:bench.close()
            closed=json.loads((bench.root/'collector_closed.json').read_text(encoding='utf-8'))
            self.assertTrue(closed['all_submitted_results_persisted']);self.assertEqual(closed['skipped'],0)
            self.assertTrue(closed['http_client']['accounting_complete'])
            self.assertLessEqual(closed['http_client']['opener_count'],2)
            self.assertEqual(closed['http_client']['calls'],bench.submitted)
            self.assertTrue(closed['own_child_exited'] and closed['collector_exited'] and closed['persistence']['writer_exited'])
            with bench.journal.db() as db:
                rows=list(db.execute('SELECT sent,received,status,error FROM requests'))
                config=json.loads(db.execute('SELECT config FROM runs').fetchone()[0])
            receipts=[json.loads(line) for line in (bench.root/'response_receipts.jsonl').read_text(encoding='utf-8').splitlines()]
            self.assertEqual(len(rows),bench.submitted);self.assertEqual(len(receipts),bench.submitted)
            self.assertEqual({(r['sent'],r['received']) for r in rows},{(r['sent'],r['received']) for r in receipts})
            self.assertTrue(all(r['status']==200 and not r['error'] for r in rows))
            self.assertEqual(config['http_client']['opener_scope'],'one per active HTTP worker')
            with self.assertRaises(ValueError):bench.start()

    def test_sealed_snapshot_remains_pipeline_compatible(self):
        from .test_cycle_pipeline import fixture
        from .cycle_pipeline import collection_info
        from . import cached_cycle_collection as module
        with workspace_fixture() as folder:
            output=Path(folder)/'fixture'
            def fake(*args,**kwargs):
                self.assertIs(kwargs['bench_factory'],CachedCycleBench);fixture(output)
                (output/'collection_complete.json').unlink() # Newly created owned test fixture only.
                shutil.copyfile(output/'application_snapshot.sqlite3',output/'testbench/application.sqlite3')
                path=output/'testbench/collector_closed.json';closed=json.loads(path.read_text(encoding='utf-8'))
                closed.update(all_submitted_results_persisted=True,http_client={'accounting_complete':True})
                path.write_text(json.dumps(closed),encoding='utf-8');return {'run':'fixture-only'}
            with patch.object(module,'base_collect',side_effect=fake):collect(output,1)
            self.assertEqual(collection_info(output)['run'],'fixture-only')

    def test_preview_never_collects(self):
        with patch('sys.argv',['cached','--order','2']),patch('monitor_service.cached_cycle_collection.collect') as call,redirect_stdout(StringIO()) as output:
            main();call.assert_not_called()
        self.assertFalse(json.loads(output.getvalue())['collection_started'])


if __name__=='__main__':unittest.main(verbosity=2)
