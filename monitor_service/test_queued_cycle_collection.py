"""Bounded persistence software tests, plus short owned loopback transport."""
from contextlib import closing,redirect_stdout
from io import StringIO
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from .queued_cycle_collection import ResultWriter,QueuedCycleBench,collect,main
from .store import Store


class WriterTests(unittest.TestCase):
    def test_all_records_exactly_once_and_copied(self):
        saved=[];errors=[];writer=ResultWriter(saved.append,errors.append,128)
        record={'id':0,'nested':[1]};writer.submit(record);record['nested'][0]=2
        for i in range(1,100):writer.submit({'id':i})
        result=writer.finish();self.assertTrue(result['complete']);self.assertEqual(result['persisted'],100)
        self.assertEqual([r['id'] for r in saved],list(range(100)));self.assertEqual(saved[0]['nested'],[1]);self.assertFalse(errors)

    def test_overflow_fails_explicitly_with_bounded_pending_records(self):
        entered=threading.Event();release=threading.Event();errors=[]
        def sink(record):entered.set();release.wait(3)
        writer=ResultWriter(sink,errors.append,1)
        try:
            writer.submit({'id':1});self.assertTrue(entered.wait(1));writer.submit({'id':2})
            with self.assertRaises(RuntimeError):writer.submit({'id':3})
            self.assertEqual(writer.queue.maxsize,1);self.assertLessEqual(writer.queue.qsize(),1);self.assertTrue(errors)
        finally:release.set();writer.thread.join(3)
        with self.assertRaises(RuntimeError):writer.finish()
        self.assertFalse(writer.status()['complete']);self.assertEqual({r['id'] for r in writer.incomplete_records()},{2,3})

    def test_write_failure_has_no_retry_and_no_complete_status(self):
        calls=[];errors=[]
        def broken(record):calls.append(record['id']);raise OSError('Controlled fixture write error')
        writer=ResultWriter(broken,errors.append);writer.submit({'id':1});writer.thread.join(2)
        with self.assertRaises(RuntimeError):writer.finish()
        self.assertEqual(calls,[1]);self.assertEqual(writer.status()['persisted'],0);self.assertTrue(errors)
        self.assertEqual(writer.incomplete_records(),[{'id':1}])

    def test_finish_waits_for_actual_sink_completion(self):
        entered=threading.Event();release=threading.Event();returned=threading.Event()
        def sink(record):entered.set();release.wait(3)
        writer=ResultWriter(sink,lambda e:None);writer.submit({'id':1});self.assertTrue(entered.wait(1))
        thread=threading.Thread(target=lambda:(writer.finish(),returned.set()));thread.start()
        try:self.assertFalse(returned.wait(.05))
        finally:release.set();thread.join(3)
        self.assertTrue(returned.is_set());self.assertTrue(writer.status()['complete'])

    def test_shutdown_timeout_is_failure_not_false_success(self):
        entered=threading.Event();release=threading.Event();errors=[]
        writer=ResultWriter(lambda r:(entered.set(),release.wait(3)),errors.append);writer.submit({'id':1});entered.wait(1)
        try:
            with self.assertRaises(RuntimeError):writer.finish(.01)
            self.assertFalse(writer.status()['complete'])
        finally:release.set();writer.thread.join(3)
        self.assertTrue(errors);self.assertFalse(writer.status()['complete'])

    def test_two_producers_one_writer_no_duplicates(self):
        saved=[];writer=ResultWriter(lambda r:saved.append(r['id']),lambda e:None,64)
        threads=[threading.Thread(target=lambda start=start:[writer.submit({'id':i}) for i in range(start,start+20)]) for start in (0,20)]
        for t in threads:t.start()
        for t in threads:t.join(2)
        self.assertTrue(writer.finish()['complete']);self.assertEqual(sorted(saved),list(range(40)))

    def test_submit_after_stop_is_explicit_failure(self):
        writer=ResultWriter(lambda r:None,lambda e:None);writer.finish()
        with self.assertRaises(RuntimeError):writer.submit({'id':1})


class BenchTests(unittest.TestCase):
    def test_slow_persistence_does_not_hold_both_http_workers(self):
        # A deterministic fixture, not an artificially slowed real run.
        from work.test_collector_diagnostic_v01 import Response
        gate=threading.Event();entered=threading.Event();saved=[]
        def sink(record):entered.set();gate.wait(3);saved.append(record)
        with tempfile.TemporaryDirectory() as folder,patch('monitor_service.queued_cycle_collection.open_direct',side_effect=lambda *a,**kw:Response()):
            bench=object.__new__(QueuedCycleBench);bench.writer=ResultWriter(sink,lambda e:None,8);bench.url='http://127.0.0.1:0'
            threads=[threading.Thread(target=bench.one_request,args=('fixture',)) for _ in range(4)]
            try:
                for t in threads:t.start()
                self.assertTrue(entered.wait(1))
                for t in threads:t.join(.3)
                self.assertTrue(all(not t.is_alive() for t in threads));self.assertEqual(bench.writer.accepted,4)
                self.assertEqual(len(saved),0)
            finally:gate.set();bench.writer.finish()
            self.assertEqual(len(saved),4)

    def test_real_short_transport_persists_all_tail_requests_and_joins_writer(self):
        with tempfile.TemporaryDirectory() as folder:
            bench=QueuedCycleBench(Store(Path(folder)/'monitor.sqlite3'))
            try:
                bench.start();bench.set_load(8,10);deadline=time.monotonic()+6
                while time.monotonic()<deadline:
                    if bench.writer.status()['persisted']>=8:break
                    time.sleep(.05)
                self.assertGreaterEqual(bench.writer.status()['persisted'],8)
            finally:bench.close()
            closed=json.loads((bench.root/'collector_closed.json').read_text(encoding='utf-8'))
            self.assertTrue(closed['all_submitted_results_persisted']);self.assertTrue(closed['persistence']['writer_exited'])
            self.assertTrue(closed['collector_exited'] and closed['own_child_exited'])
            with bench.journal.db() as db:
                rows=list(db.execute('SELECT sent,received,status,error FROM requests'));config=json.loads(db.execute('SELECT config FROM runs').fetchone()[0])
            receipts=[json.loads(r) for r in (bench.root/'response_receipts.jsonl').read_text(encoding='utf-8').splitlines()]
            self.assertEqual(len(rows),bench.submitted);self.assertEqual(len(receipts),bench.submitted)
            self.assertEqual({(r['sent'],r['received']) for r in rows},{(r['sent'],r['received']) for r in receipts})
            self.assertEqual(config['persistence']['http_workers'],2)
            with self.assertRaises(ValueError):bench.start()

    def test_preview_does_not_collect(self):
        with patch('sys.argv',['queued','--order','2']),patch('monitor_service.queued_cycle_collection.collect') as call,redirect_stdout(StringIO()) as output:
            main();call.assert_not_called()
        self.assertFalse(json.loads(output.getvalue())['collection_started'])

    def test_closed_snapshot_is_compatible_with_existing_pipeline(self):
        from .test_cycle_pipeline import fixture
        from .cycle_pipeline import collection_info
        from . import queued_cycle_collection as module
        with tempfile.TemporaryDirectory() as folder:
            output=Path(folder)/'fixture'
            def fake_base(*args,**kwargs):
                self.assertIs(kwargs['bench_factory'],QueuedCycleBench);fixture(output)
                (output/'collection_complete.json').unlink() # Only this freshly made test fixture.
                shutil.copyfile(output/'application_snapshot.sqlite3',output/'testbench/application.sqlite3')
                path=output/'testbench/collector_closed.json';closed=json.loads(path.read_text(encoding='utf-8'))
                closed['all_submitted_results_persisted']=True;closed['persistence']={'complete':True}
                path.write_text(json.dumps(closed),encoding='utf-8');return {'run':'fixture-only'}
            with patch.object(module,'base_collect',side_effect=fake_base):collect(output,1)
            self.assertEqual(collection_info(output)['run'],'fixture-only')


if __name__=='__main__':unittest.main(verbosity=2)
