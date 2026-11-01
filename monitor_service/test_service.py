"""Software regression/integration tests, NOT new scientific experiments."""
import unittest,tempfile,json,time,threading,urllib.request,urllib.error,sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from .detector import Detector,np
from .store import Store,Conflict,encode
from .server import Server

ROOT=Path(__file__).resolve().parents[1]
def samples(n,d=6):
    rng=np.random.default_rng(73019)
    latent=rng.normal(size=(n,2));matrix=rng.normal(size=(2,d))
    result=latent@matrix+.3*rng.normal(size=(n,d))
    if n>912:result[904:912,0]+=10
    return result
def observations(raw,start=1):
    return [{'timestamp':start+i,'values':{f'f{j}':float(v) for j,v in enumerate(row)}} for i,row in enumerate(raw)]
def feed(store,host,raw,start=1):
    result=[]
    rows=observations(raw,start)
    for a in range(0,len(rows),128):result.extend(store.ingest(host,{'observations':rows[a:a+128]})['decisions'])
    return result

class ServiceTests(unittest.TestCase):
    def setUp(self):
        parent=ROOT/'work/service-test-tmp';parent.mkdir(exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(dir=parent);self.store=Store(Path(self.temp.name)/'test.sqlite3')
    def tearDown(self):
        self.assertTrue(Path(self.temp.name).resolve().is_relative_to((ROOT/'work/service-test-tmp').resolve()))
        self.temp.cleanup()
    def register(self,host='one',d=6):return self.store.register({'id':host,'features':[f'f{i}' for i in range(d)]})

    def test_reference_a0_predictions_and_cuts(self):
        # Reference detector itself needs NumPy only; do not import unrelated
        # historical experiment harnesses that also require SciPy statistics.
        frozen=ROOT/'outputs/freeze-v1/code'
        sys.path.insert(0,str(frozen))
        from stage2_calibration import accelerated as reference_engine
        from stage2_calibration.generator import FeatureParts
        from stage2_calibration.procedures import run_policy
        self.assertTrue(Path(reference_engine.__file__).resolve().is_relative_to(frozen.resolve()))
        raw=samples(976,38)
        parts=FeatureParts(raw[:256],raw[256:384],raw[384:896],raw[896:])
        shared=reference_engine.compute_common(parts)
        expected=run_policy('A0',**shared['test'],initial_records=shared['histories'][False],window=32,fraction=.25)
        detector=Detector([f'f{i}' for i in range(38)])
        results=[]
        for i,row in enumerate(raw):results.extend(detector.observe(i+1,row))
        actual=np.array([r['alarm'] for r in results])
        cuts=np.array([np.inf if results[i]['auxiliary_threshold'] is None else results[i]['auxiliary_threshold'] for i in range(0,len(results),16)])
        self.assertEqual(actual.tobytes(),expected['prediction'].tobytes())
        self.assertEqual(cuts.tobytes(),expected['cuts'].tobytes())
        self.assertEqual(detector.channel_scales.tobytes(),shared['channel_scales'].tobytes())
        self.assertEqual(detector.er_scales.tobytes(),shared['er_scales'].tobytes())

    def test_restart_pending_and_future_causality(self):
        raw=samples(960)
        first=Detector([f'f{i}' for i in range(6)])
        for i in range(903):first.observe(i+1,raw[i])
        restored=Detector.from_state(json.loads(encode(first.to_state())))
        left=[];right=[]
        for i in range(903,len(raw)):
            left.extend(first.observe(i+1,raw[i]));right.extend(restored.observe(i+1,raw[i]))
        self.assertEqual(encode(left),encode(right));self.assertEqual(encode(first.to_state()),encode(restored.to_state()))
        # Later deliveries cannot alter already issued decisions or their thresholds.
        changed=raw.copy();changed[928:]+=100
        a=Detector([f'f{i}' for i in range(6)]);b=Detector([f'f{i}' for i in range(6)])
        aa=[];bb=[]
        for i in range(len(raw)):
            aa.extend(a.observe(i+1,raw[i]));bb.extend(b.observe(i+1,changed[i]))
        self.assertEqual(encode([r for r in aa if r['timestamp']<=928]),encode([r for r in bb if r['timestamp']<=928]))
        # The changed delivery cannot set its own threshold.
        self.assertEqual(aa[32]['auxiliary_threshold'],bb[32]['auxiliary_threshold'])

    def test_sqlite_atomicity_and_restart(self):
        self.register();raw=samples(960);feed(self.store,'one',raw[:903])
        before=self.store.state('one')
        invalid=observations(raw[903:905],904);invalid[-1]['values']['f0']=None
        with self.assertRaises(ValueError):self.store.ingest('one',{'observations':invalid})
        self.assertEqual(before,self.store.state('one'))
        # Process-equivalent reconstruction from persisted JSON state and pending rows.
        reopened=Store(self.store.path)
        actual=feed(reopened,'one',raw[903:],904)
        control=Detector([f'f{i}' for i in range(6)]);expected=[]
        for i,row in enumerate(raw):expected.extend(control.observe(i+1,row))
        self.assertEqual([r['alarm'] for r in actual],[r['alarm'] for r in expected])
        self.assertEqual(len(reopened.history('one',512)['observations']),512)
        with self.assertRaises(Conflict):feed(reopened,'one',raw[:1],960)
        self.assertEqual(reopened.state('one')[0]['samples'],960)

    def test_isolated_hosts_concurrent_ingest(self):
        self.register('a');self.register('b');raw=samples(928)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda h:feed(self.store,h,raw),('a','b')))
        for key in ('timestamp','alarm','auxiliary_threshold'):
            self.assertEqual([r[key] for r in results[0]],[r[key] for r in results[1]])
        self.assertEqual([h['samples'] for h in self.store.state()],[928,928])

    def test_reject_bad_schema_and_values_without_writes(self):
        self.register()
        for bad in (float('nan'),float('inf'),True,None,10**1000):
            row=observations(samples(1))[0];row['values']['f0']=bad
            with self.assertRaises(ValueError):self.store.ingest('one',{'observations':[row]})
        self.assertEqual(self.store.state('one')[0]['samples'],0)
        with self.assertRaises(ValueError):self.store.register({'id':'bad','features':['a','a']})
        with self.assertRaises(ValueError):Detector(['a','b'],components=2)

    def test_real_os_collector_read_only(self):
        from .collector import metrics
        collector=metrics();time.sleep(.03);values=collector.sample()
        self.assertGreaterEqual(len(values),2)
        self.assertTrue(all(np.isfinite(v) for v in values.values()))
        self.assertTrue(0<=values['cpu_percent']<=100)
        self.assertTrue(0<=values['memory_percent']<=100)

    def test_http_full_ingest_and_persistence(self):
        server=Server(('127.0.0.1',0),self.store)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        url=f'http://127.0.0.1:{server.server_port}'
        def request(path,data=None,headers=None):
            r=urllib.request.Request(url+path,data=None if data is None else encode(data).encode(),headers=headers or ({'Content-Type':'application/json'} if data is not None else {}))
            with urllib.request.urlopen(r,timeout=30) as response:return response.status,response.read()
        try:
            self.assertEqual(request('/api/health')[0],200)
            self.assertIn('Наблюдение за сервером',request('/')[1].decode())
            status,_=request('/api/hosts',{'id':'web','features':[f'f{i}' for i in range(6)]});self.assertEqual(status,201)
            rows=observations(samples(960))
            for a in range(0,960,128):request('/api/hosts/web/observations',{'observations':rows[a:a+128]})
            state=json.loads(request('/api/state')[1]);self.assertEqual(state['hosts'][0]['samples'],960)
            history=json.loads(request('/api/hosts/web/history')[1]);self.assertEqual(sum(r['decision'] is not None for r in history['observations']),64)
            self.assertTrue(all(r['decision']['decision_source_time']>=r['timestamp'] for r in history['observations'] if r['decision']))
            # Immediate mode also travels through the real API, in a separate
            # disposable fixture; source data are not user application records.
            request('/api/hosts',{'id':'online','features':[f'f{i}' for i in range(4)],
                                'settings':{'components':2,'processing_mode':'per_observation_v1'}})
            online=observations(samples(900,4))
            for a in range(0,896,128):request('/api/hosts/online/observations',{'observations':online[a:a+128]})
            out=json.loads(request('/api/hosts/online/observations',{'observations':online[896:897]})[1])
            self.assertEqual(len(out['decisions']),1)
            self.assertEqual(out['decisions'][0]['decision_source_time'],897)
            online_state=next(h for h in json.loads(request('/api/state')[1])['hosts'] if h['id']=='online')
            self.assertEqual(online_state['delivery_rows'],1);self.assertEqual(online_state['pending'],1)
            with self.assertRaises(urllib.error.HTTPError) as caught:request('/api/hosts/web/observations',{'observations':rows[:1]})
            self.assertEqual(caught.exception.code,409)
            with self.assertRaises(urllib.error.HTTPError) as caught:request('/api/demo',{}, {'Content-Type':'application/json','Origin':'https://foreign.example'})
            self.assertEqual(caught.exception.code,403)
            # Start the real collector once; stop cleanly. No application stress/faults.
            request('/api/local/start',{})
            deadline=time.time()+4
            while time.time()<deadline:
                local=[h for h in self.store.state() if h['id']=='local-pc']
                if local and local[0]['samples']>0:break
                time.sleep(.1)
            self.assertGreater(self.store.state('local-pc')[0]['samples'],0)
        finally:server.close();thread.join(timeout=5)
        self.assertEqual(Store(self.store.path).state('web')[0]['samples'],960)

if __name__=='__main__':unittest.main(verbosity=2)
