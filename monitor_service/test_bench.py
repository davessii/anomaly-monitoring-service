"""Technical application/transport tests. No anomaly-quality conclusions."""
import unittest,tempfile,threading,time,json,urllib.error,sqlite3
from pathlib import Path
from unittest.mock import patch
from .bench import Bench,FEATURES,detector_values,get_json,command,open_direct
from .bench_journal import Journal
from .test_app import Application,ApplicationState
from .store import Store
from .server import Server

ROOT=Path(__file__).resolve().parents[1]

class BenchTests(unittest.TestCase):
    def setUp(self):
        root=ROOT/'work/bench-test-tmp';root.mkdir(exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(dir=root)
        self.store=Store(Path(self.temp.name)/'monitor.sqlite3')
    def tearDown(self):
        self.assertTrue(Path(self.temp.name).resolve().is_relative_to((ROOT/'work/bench-test-tmp').resolve()))
        self.temp.cleanup()
    def test_metrics_derivation_excludes_outcomes(self):
        a={'timestamp':10,'cpu_seconds':1,'requests_started':4,'requests_completed':3,'active_requests':1,'response_bytes':120,'memory_mib':30}
        b=dict(a,timestamp=12,cpu_seconds=1.2,requests_started=8,requests_completed=7,active_requests=1,response_bytes=280,memory_mib=31,
           status=503,response_time_ms=250,served_mode='delay',control_mode='error',extra_cpu_seconds=4,
           extra_work_requests=100,extra_work_iterations=5000,client_target_rps=8,client_skipped_slots=5)
        values=detector_values(a,b)
        self.assertEqual(tuple(values),FEATURES)
        self.assertAlmostEqual(values['app_cpu_percent'],10)
        self.assertEqual(values['completed_per_second'],2)
        self.assertNotIn('started_per_second',values)
        self.assertNotIn('response_bytes_per_second',values)
        self.assertFalse(set(values)&{'status','response_time_ms','served_mode','control_mode','extra_cpu_seconds','extra_work_requests','extra_work_iterations','client_target_rps','client_skipped_slots'})
        with self.assertRaises(ValueError):detector_values(a,a)
    def test_problem_expires_and_limits(self):
        state=ApplicationState();state.set_mode('delay',1)
        self.assertEqual(state.current_mode(),'delay')
        time.sleep(1.02)
        self.assertEqual(state.current_mode(),'normal')
        for duration in (0,121,True,float('nan')):
            with self.assertRaises(ValueError):state.set_mode('delay',duration)
    def test_journal_keeps_outcomes_controls_and_interruption(self):
        path=Path(self.temp.name)/'journal.sqlite3';j=Journal(path)
        j.start('r','h',{'scope':'technical'})
        j.request('r',1,1.25,250,503,None,'error')
        j.control('r','error',10)
        j.sample('r',{'timestamp':1},{'a':1},0)
        runs=j.runs();self.assertEqual(runs[0]['error_responses'],1)
        self.assertEqual(runs[0]['recent_requests'][0]['served_mode'],'error')
        restarted=Journal(path)
        self.assertEqual(restarted.runs()[0]['status'],'interrupted')
        with j.db() as db:
            self.assertEqual(set(json.loads(db.execute('SELECT detector_values FROM samples').fetchone()[0])),{'a'})
    def test_application_http_normal_delay_error_and_control_auth(self):
        app=Application();thread=threading.Thread(target=app.serve_forever,daemon=True);thread.start()
        url=f'http://127.0.0.1:{app.server_port}'
        try:
            self.assertEqual(get_json(url+'/health')['role'],'owned-test-application')
            with open_direct(url+'/work') as response:
                self.assertEqual(response.status,200);self.assertEqual(response.headers['X-Workbench-Mode'],'normal');response.read()
            with self.assertRaises(urllib.error.HTTPError) as denied:command(url+'/control','wrong',{'mode':'delay','seconds':1})
            self.assertEqual(denied.exception.code,403);denied.exception.close()
            command(url+'/control',app.token,{'mode':'delay','seconds':1})
            clock=time.perf_counter()
            with open_direct(url+'/work') as response:
                self.assertEqual(response.headers['X-Workbench-Mode'],'delay');response.read()
            self.assertGreaterEqual(time.perf_counter()-clock,.20)
            command(url+'/control',app.token,{'mode':'error','seconds':1})
            with self.assertRaises(urllib.error.HTTPError) as injected:open_direct(url+'/work')
            self.assertEqual(injected.exception.code,503);self.assertEqual(injected.exception.headers['X-Workbench-Mode'],'error');injected.exception.close()
            command(url+'/control',app.token,{'mode':'normal','seconds':1})
            self.assertIn('result',get_json(url+'/work'))
            metrics=get_json(url+'/metrics',app.token)
            self.assertEqual(metrics['requests_started'],4)
            self.assertEqual(metrics['requests_completed'],4)
            self.assertEqual(metrics['active_requests'],0)
            self.assertGreater(metrics['memory_mib'],0)
        finally:app.close();thread.join(timeout=5)
    def test_control_refused_until_model_ready(self):
        bench=Bench(self.store)
        self.store.register({'id':'case','features':list(FEATURES),'settings':{'components':2}})
        bench.journal.start('case','case',{})
        bench.current={'id':'case','host':'case','running':True};bench.url='http://127.0.0.1:1';bench.token='test'
        try:
            with patch('monitor_service.bench.command',return_value={'mode':'delay'}) as send:
                with self.assertRaises(ValueError):bench.control('delay',1)
                send.assert_not_called()
                # Pure software bootstrap fixture, NOT collected app data or quality test.
                import math
                rows=[{'timestamp':i+1,'values':dict(zip(FEATURES,[math.sin(i*.1+j)+.1*math.cos(i*.17+j*2) for j in range(len(FEATURES))]))} for i in range(896)]
                for a in range(0,len(rows),128):self.store.ingest('case',{'observations':rows[a:a+128]})
                self.assertEqual(bench.control('delay',1),{'mode':'delay'})
                send.assert_called_once()
                self.assertEqual(bench.journal.runs()[0]['controls'][0]['mode'],'delay')
        finally:bench.close()
    def test_owned_child_workload_sampling_and_stop(self):
        bench=Bench(self.store)
        try:
            current=bench.start();process=bench.process
            self.assertIsNotNone(process);self.assertIsNone(process.poll())
            self.assertNotEqual(process.pid,__import__('os').getpid())
            deadline=time.monotonic()+8
            while time.monotonic()<deadline:
                run=bench.journal.runs()[0]
                if run['sample_count']>=2 and run['request_count']>=3:break
                time.sleep(.1)
            self.assertGreaterEqual(run['sample_count'],2);self.assertGreaterEqual(run['request_count'],3)
            self.assertIn('numpy',run['config']['runtime'])
            self.assertFalse(run['config']['runtime']['active_blas_threads_verified'])
            self.assertTrue(all(r['status']==200 and r['served_mode']=='normal' for r in run['recent_requests']))
            with self.assertRaises(ValueError):bench.control('delay',1)
            state=self.store.state(current['host'])[0]
            self.assertEqual(state['features'],list(FEATURES));self.assertEqual(state['phase'],'warmup')
            self.assertEqual(state['source'],'application')
            self.assertEqual(state['processing_mode'],'per_observation_v1')
            self.assertEqual(state['delivery_rows'],1)
            with bench.journal.db() as db:
                for record in db.execute('SELECT detector_values FROM samples'):
                    self.assertEqual(set(json.loads(record[0])),set(FEATURES))
            # Transport-only test in this temporary fixture: exercise the owned
            # child directly. Production UI/API training gate remains checked above.
            command(bench.url+'/control',bench.token,{'mode':'delay','seconds':1})
            bench.one_request(current['id'])
            delayed=[r for r in bench.journal.runs()[0]['recent_requests'] if r['served_mode']=='delay']
            self.assertTrue(delayed);self.assertGreaterEqual(delayed[-1]['duration_ms'],200)
            command(bench.url+'/control',bench.token,{'mode':'error','seconds':1})
            bench.one_request(current['id'])
            self.assertTrue(any(r['status']==503 for r in bench.journal.runs()[0]['recent_requests']))
            command(bench.url+'/control',bench.token,{'mode':'extra_cpu','seconds':1})
            bench.one_request(current['id'])
            extra=[r for r in bench.journal.runs()[0]['recent_requests'] if r['served_mode']=='extra_cpu']
            self.assertTrue(extra);self.assertGreater(extra[-1]['extra_iterations'],0)
            self.assertGreaterEqual(extra[-1]['extra_cpu_ms'],0)
            command(bench.url+'/control',bench.token,{'mode':'normal','seconds':1})
            bench.stop();bench.thread.join(timeout=12)
            self.assertFalse(bench.thread.is_alive());self.assertIsNotNone(process.poll())
            self.assertEqual(bench.journal.runs()[0]['status'],'stopped')
        finally:bench.close()
    def test_monitor_api_routes_and_shutdown_cleanup(self):
        server=Server(('127.0.0.1',0),self.store)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        url=f'http://127.0.0.1:{server.server_port}'
        def post(path,data):
            import urllib.request
            req=urllib.request.Request(url+path,data=json.dumps(data).encode(),headers={'Content-Type':'application/json'})
            with open_direct(req,timeout=15) as response:return json.load(response)
        try:
            health=get_json(url+'/api/health')
            self.assertEqual(health['version'],'0.7.0')
            self.assertTrue(health['application_access_log'])
            self.assertEqual(get_json(url+'/api/bench')['runs'],[])
            with open_direct(url+'/bench.js') as response:self.assertIn('bench-start',response.read().decode())
            run=post('/api/bench/start',{})
            process=server.bench.process
            self.assertEqual(get_json(url+'/api/bench')['current']['id'],run['id'])
            with self.assertRaises(urllib.error.HTTPError) as denied:post('/api/bench/control',{'mode':'delay','seconds':1})
            self.assertEqual(denied.exception.code,400);denied.exception.close()
            self.assertEqual(post('/api/bench/load',{'rate':8,'seconds':1})['rate'],8)
            self.assertEqual(get_json(url+'/api/bench')['current']['target_requests_per_second'],8)
            post('/api/bench/control',{'mode':'normal','seconds':1})
            self.assertEqual(get_json(url+'/api/bench')['current']['target_requests_per_second'],2)
            with self.assertRaises(urllib.error.HTTPError) as bad_load:post('/api/bench/load',{'rate':100,'seconds':1})
            self.assertEqual(bad_load.exception.code,400);bad_load.exception.close()
        finally:server.close();thread.join(timeout=5)
        self.assertIsNotNone(process.poll())
        self.assertFalse(server.bench.thread.is_alive())

if __name__=='__main__':unittest.main(verbosity=2)
