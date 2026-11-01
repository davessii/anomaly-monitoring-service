"""Bounded work/load control tests; no anomaly-quality or load-stress experiment."""
import json,tempfile,threading,time,unittest
from pathlib import Path
from unittest.mock import patch
from .test_app import Application,ApplicationState,extra_work
from .bench import Bench,FEATURES,get_json,command,open_direct
from .store import Store
from .test_service import samples

ROOT=Path(__file__).resolve().parents[1]

class WorkControlTests(unittest.TestCase):
    def setUp(self):
        parent=ROOT/'work/controls-test-tmp';parent.mkdir(exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(dir=parent)
        self.store=Store(Path(self.temp.name)/'monitor.sqlite3')
    def tearDown(self):self.temp.cleanup()
    def fixture(self,ready=False):
        bench=Bench(self.store)
        self.store.register({'id':'case','features':list(FEATURES),'settings':{'components':2}})
        bench.journal.start('case','case',{})
        bench.current={'id':'case','host':'case','running':True};bench.url='http://127.0.0.1:1';bench.token='fixture'
        if ready:
            data=samples(896,len(FEATURES))
            for start in range(0,len(data),128):
                self.store.ingest('case',{'observations':[{'timestamp':i+1,'values':dict(zip(FEATURES,data[i].tolist()))} for i in range(start,min(start+128,len(data)))]})
        return bench

    def test_extra_work_honours_cancellation_cpu_and_wall_caps(self):
        self.assertEqual(extra_work(lambda:False)['iterations'],0)
        with patch('monitor_service.test_app.time.thread_time',side_effect=[0,1,1]),patch('monitor_service.test_app.time.perf_counter',side_effect=[0,0]):
            self.assertEqual(extra_work(lambda:True)['iterations'],0)
        with patch('monitor_service.test_app.time.thread_time',return_value=0),patch('monitor_service.test_app.time.perf_counter',side_effect=[0,1,1]):
            self.assertEqual(extra_work(lambda:True)['iterations'],0)
        checks=iter([True,False])
        self.assertEqual(extra_work(lambda:next(checks))['iterations'],32)

    def test_extra_mode_expires_and_invalid_duration_is_rejected(self):
        state=ApplicationState()
        with patch('monitor_service.test_app.time.monotonic',return_value=100) as clock:
            state.set_mode('extra_cpu',2);self.assertEqual(state.current_mode(),'extra_cpu')
            clock.return_value=103;self.assertEqual(state.current_mode(),'normal')
        for bad in (0,121,True,float('nan'),float('inf')):
            with self.assertRaises(ValueError):state.set_mode('extra_cpu',bad)

    def test_http_extra_work_preserves_response_and_records_actual_execution(self):
        app=Application();thread=threading.Thread(target=app.serve_forever,daemon=True);thread.start()
        url=f'http://127.0.0.1:{app.server_port}'
        try:
            baseline=get_json(url+'/work')
            command(url+'/control',app.token,{'mode':'extra_cpu','seconds':1})
            with open_direct(url+'/work') as response:
                actual=json.load(response);cpu=float(response.headers['X-Workbench-Extra-CPU-Ms']);iterations=int(response.headers['X-Workbench-Extra-Iterations'])
                self.assertEqual(response.headers['X-Workbench-Mode'],'extra_cpu');self.assertEqual(response.status,200)
            self.assertEqual(actual,baseline);self.assertGreater(iterations,0);self.assertGreaterEqual(cpu,0)
            metrics=get_json(url+'/metrics',app.token)
            self.assertEqual(metrics['extra_work_requests'],1);self.assertEqual(metrics['extra_work_iterations'],iterations)
            self.assertAlmostEqual(metrics['extra_cpu_seconds']*1000,cpu,places=6)
            command(url+'/control',app.token,{'mode':'normal','seconds':1})
            self.assertEqual(get_json(url+'/work'),baseline)
            self.assertEqual(get_json(url+'/metrics',app.token)['extra_work_requests'],1)
        finally:app.close();thread.join(timeout=5)

    def test_load_change_allowed_during_learning_expires_and_is_logged(self):
        bench=self.fixture()
        try:
            with patch('monitor_service.bench.time.monotonic',return_value=100) as clock:
                bench.set_load(8,2);self.assertEqual(bench.effective_load(),8)
                clock.return_value=103;self.assertEqual(bench.effective_load(),2)
            self.assertEqual(bench.journal.runs()[0]['load_changes'][0]['rate'],8)
            before=bench.journal.runs()[0]['load_changes']
            for bad in (True,0,3,100,float('nan'),float('inf'),'8'):
                with self.assertRaises(ValueError):bench.set_load(bad,1)
            self.assertEqual(bench.journal.runs()[0]['load_changes'],before)
            self.assertEqual(self.store.state('case')[0]['samples'],0)
            with self.assertRaises(ValueError):bench.control('extra_cpu',1)
        finally:bench.close()

    def test_load_and_extra_work_are_separate_and_normal_resets_both(self):
        bench=self.fixture(ready=True)
        try:
            with patch('monitor_service.bench.command',return_value={'mode':'normal'}) as send:
                bench.set_load(8,10)
                with self.assertRaises(ValueError):bench.control('extra_cpu',10)
                send.assert_not_called()
                bench.set_load(2,10);bench.control('extra_cpu',10)
                with self.assertRaises(ValueError):bench.set_load(8,10)
                bench.control('normal',1)
                self.assertEqual(bench.mode,'normal');self.assertEqual(bench.effective_load(),2)
                self.assertEqual(bench.journal.runs()[0]['controls'][0]['mode'],'normal')
        finally:bench.close()

    def test_load_has_at_most_two_tasks_and_skips_instead_of_queueing(self):
        bench=self.fixture();guard=threading.Lock();active=0;peak=0
        def slow_request(run):
            nonlocal active,peak
            with guard:active+=1;peak=max(peak,active)
            time.sleep(.35)
            with guard:active-=1
        bench.one_request=slow_request;bench.set_load(8,2)
        thread=threading.Thread(target=bench.workload,args=('case',));thread.start()
        try:
            deadline=time.monotonic()+2
            while time.monotonic()<deadline:
                with bench.lock:skipped=bench.skipped
                if skipped:break
                time.sleep(.02)
            self.assertGreater(skipped,0)
            self.assertLessEqual(peak,2)
            with bench.lock:self.assertEqual(bench.scheduled,bench.submitted+bench.skipped)
        finally:bench.stop();thread.join(timeout=5);bench.close()
        self.assertFalse(thread.is_alive());self.assertEqual(active,0)

if __name__=='__main__':unittest.main(verbosity=2)
