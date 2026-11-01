"""Technical tests of additive raw cycle snapshots, not detection quality."""
import ctypes
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from .process_cycles import ProcessCycles,validate_bracket
from .test_app import ApplicationState
from .bench import Bench,FEATURES,detector_values,get_json
from .store import Store

ROOT=Path(__file__).resolve().parents[1]


class FakeReader:
    available=True
    def __init__(self,values):self.values=iter(values)
    def read(self):
        result=next(self.values)
        if isinstance(result,BaseException):raise result
        return result


class CycleSnapshotTests(unittest.TestCase):
    def test_locked_count_snapshot_and_existing_fields(self):
        state=ApplicationState(cycle_reader=FakeReader([1000,1040]))
        state.started=18;state.completed=17;state.active=1
        def rss():
            self.assertFalse(state.lock.locked())
            return 32.
        with patch('monitor_service.test_app.memory_mib',side_effect=rss):
            value=state.metrics()
        self.assertEqual(value['cpu_cycles'],1000)
        self.assertEqual(value['cpu_cycles_after_snapshot'],1040)
        self.assertEqual(value['cpu_cycles_status'],'available')
        self.assertIsNone(value['cpu_cycles_error'])
        self.assertEqual(value['cpu_cycles_unit'],'cycles')
        self.assertGreaterEqual(value['cpu_cycles_read_span_ns'],0)
        self.assertEqual(value['requests_completed'],17)
        self.assertEqual(value['active_requests'],1)
        self.assertGreaterEqual(value['cpu_seconds'],0)
        self.assertEqual(value['memory_mib'],32)

    def test_writer_cannot_change_counts_between_cycle_reads(self):
        state=ApplicationState(cycle_reader=FakeReader([0,1]))
        state.completed=17
        attempted=threading.Event();finished=threading.Event();threads=[]
        class Reader:
            available=True
            calls=0
            def read(reader):
                self.assertTrue(state.lock.locked())
                reader.calls+=1
                if reader.calls==1:
                    def writer():
                        attempted.set()
                        with state.lock:state.completed+=1
                        finished.set()
                    worker=threading.Thread(target=writer);threads.append(worker);worker.start()
                    self.assertTrue(attempted.wait(1))
                self.assertFalse(finished.is_set())
                return reader.calls*1000
        state.cycle_reader=Reader()
        with patch('monitor_service.test_app.memory_mib',return_value=30):value=state.metrics()
        for worker in threads:worker.join(timeout=2)
        self.assertTrue(finished.is_set())
        self.assertEqual(value['requests_completed'],17)
        self.assertEqual(state.completed,18)

    def test_unsupported_is_none_not_zero(self):
        with patch('monitor_service.process_cycles.os.name','posix'):
            reader=ProcessCycles()
            self.assertFalse(reader.available)
            self.assertIsNone(reader.read())
        state=ApplicationState(cycle_reader=reader)
        with patch('monitor_service.test_app.memory_mib',return_value=30):value=state.metrics()
        self.assertEqual(value['cpu_cycles_status'],'unsupported')
        self.assertIsNone(value['cpu_cycles']);self.assertIsNone(value['cpu_cycles_after_snapshot'])

    def test_query_failure_does_not_reuse_old_values_and_can_recover(self):
        state=ApplicationState(cycle_reader=FakeReader([100,110,OSError('fixture'),200,210]))
        with patch('monitor_service.test_app.memory_mib',return_value=30):
            good=state.metrics();bad=state.metrics();recovered=state.metrics()
        self.assertEqual(good['cpu_cycles_status'],'available')
        self.assertEqual(bad['cpu_cycles_status'],'error')
        self.assertIn('fixture',bad['cpu_cycles_error'])
        self.assertIsNone(bad['cpu_cycles']);self.assertIsNone(bad['cpu_cycles_after_snapshot'])
        self.assertEqual(recovered['cpu_cycles'],200)
        self.assertIsNone(recovered['cpu_cycles_error'])

    def test_second_read_failure_discards_partial_bracket(self):
        state=ApplicationState(cycle_reader=FakeReader([100,OSError('second')]))
        with patch('monitor_service.test_app.memory_mib',return_value=30):value=state.metrics()
        self.assertEqual(value['cpu_cycles_status'],'error')
        self.assertIsNone(value['cpu_cycles']);self.assertIsNone(value['cpu_cycles_after_snapshot'])
        self.assertGreaterEqual(value['cpu_seconds'],0)

    def test_invalid_cycle_values_are_not_clamped(self):
        for pair in ((100,99),(-1,100),(True,100),(1.5,100),(100,None)):
            state=ApplicationState(cycle_reader=FakeReader(pair))
            with patch('monitor_service.test_app.memory_mib',return_value=30):value=state.metrics()
            self.assertEqual(value['cpu_cycles_status'],'error',pair)
            self.assertIsNone(value['cpu_cycles'])
        validate_bracket(0,0) # An actual zero is distinct from unsupported.

    def test_initialization_failure_is_explicit_and_preserves_old_metrics(self):
        with patch('monitor_service.test_app.ProcessCycles',side_effect=OSError('init')):
            state=ApplicationState()
        with patch('monitor_service.test_app.memory_mib',return_value=30):value=state.metrics()
        self.assertEqual(value['cpu_cycles_status'],'error')
        self.assertIn('init',value['cpu_cycles_error'])
        self.assertIn('cpu_seconds',value)

    @unittest.skipUnless(os.name=='nt','Windows native cycle API')
    def test_native_self_counter_is_integer_and_nondecreasing(self):
        reader=ProcessCycles();before=reader.read();after=reader.read()
        self.assertIs(type(before),int);self.assertGreaterEqual(before,0)
        self.assertGreaterEqual(after,before)

    def test_old_detector_inputs_do_not_depend_on_new_fields(self):
        a={'timestamp':1,'cpu_seconds':1,'requests_completed':1,'memory_mib':30,'active_requests':0}
        b=dict(a,timestamp=2,cpu_seconds=1.1,requests_completed=3)
        old=detector_values(a,b)
        extended=detector_values(dict(a,cpu_cycles=100),dict(b,cpu_cycles=10**15,cpu_cycles_status='error'))
        self.assertEqual(old,extended)
        self.assertEqual(tuple(old),FEATURES)

    @unittest.skipUnless(os.name=='nt','Windows live child')
    def test_owned_child_http_bracket_journal_and_stop(self):
        parent=ROOT/'work/cycle-counter-instrumentation-v01/live'
        parent.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(dir=parent) as temporary:
            store=Store(Path(temporary)/'monitor.sqlite3');bench=Bench(store)
            process=None;handle=None
            from ctypes import wintypes as w
            kernel=ctypes.WinDLL('kernel32',use_last_error=True)
            kernel.OpenProcess.argtypes=[w.DWORD,w.BOOL,w.DWORD];kernel.OpenProcess.restype=w.HANDLE
            kernel.QueryProcessCycleTime.argtypes=[w.HANDLE,ctypes.POINTER(ctypes.c_ulonglong)]
            kernel.QueryProcessCycleTime.restype=w.BOOL
            kernel.CloseHandle.argtypes=[w.HANDLE]
            def external():
                value=ctypes.c_ulonglong()
                if not kernel.QueryProcessCycleTime(handle,ctypes.byref(value)):raise ctypes.WinError(ctypes.get_last_error())
                return value.value
            try:
                run=bench.start();process=bench.process
                handle=kernel.OpenProcess(0x1000,False,process.pid)
                self.assertTrue(handle)
                before=external();raw=get_json(bench.url+'/metrics',bench.token);after=external()
                self.assertEqual(raw['cpu_cycles_status'],'available')
                self.assertLessEqual(before,raw['cpu_cycles'])
                self.assertLessEqual(raw['cpu_cycles'],raw['cpu_cycles_after_snapshot'])
                self.assertLessEqual(raw['cpu_cycles_after_snapshot'],after)
                deadline=time.monotonic()+8
                while time.monotonic()<deadline:
                    with bench.journal.db() as db:rows=list(db.execute('SELECT counters,detector_values FROM samples ORDER BY id'))
                    if len(rows)>=2:break
                    time.sleep(.05)
                self.assertGreaterEqual(len(rows),2)
                previous=None
                for row in rows:
                    counters=json.loads(row['counters'])
                    self.assertEqual(counters['metrics_schema'],'application_cycles_snapshot_v1')
                    self.assertEqual(counters['cpu_cycles_status'],'available')
                    self.assertIs(type(counters['cpu_cycles']),int)
                    self.assertLessEqual(counters['cpu_cycles'],counters['cpu_cycles_after_snapshot'])
                    if previous is not None:self.assertGreaterEqual(counters['cpu_cycles'],previous)
                    previous=counters['cpu_cycles_after_snapshot']
                    self.assertEqual(set(json.loads(row['detector_values'])),set(FEATURES))
                self.assertEqual(store.state(run['host'])[0]['phase'],'warmup')
                bench.stop();bench.thread.join(timeout=12)
                self.assertFalse(bench.thread.is_alive());self.assertIsNotNone(process.poll())
            finally:
                bench.close()
                if handle:kernel.CloseHandle(handle)
            self.assertIsNotNone(process.poll())
            evidence={'scope':'Short technical HTTP/journal snapshot check, no detection evaluation.',
                      'child_pid':process.pid,'child_exited':process.poll() is not None,
                      'collector_thread_exited':not bench.thread.is_alive(),
                      'phase':'warmup','ipca_initialization_completed':False,
                      'model_features':list(FEATURES),'full_window_admission_tested':False,
                      'external_before':before,'http_metrics':raw,'external_after':after,
                      'saved_counters':[json.loads(row['counters']) for row in rows]}
            (parent.parent/'live_evidence.json').write_text(json.dumps(evidence,ensure_ascii=False,indent=2),encoding='utf-8')


if __name__=='__main__':unittest.main(verbosity=2)
