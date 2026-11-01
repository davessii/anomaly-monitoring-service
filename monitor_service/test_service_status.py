"""Fast fabricated status/incident fixtures, not IPCA quality experiments."""
import json,math,tempfile,threading,time,unittest,urllib.error
from pathlib import Path
from .service_status import ServicePolicy,response_cause,group_events,model_events,build_status
from .store import Store
from .server import Server
from .bench import get_json,open_direct

ROOT=Path(__file__).resolve().parents[1]

def request_row(t,duration=50,status=200,error=None,mode='normal'):
    return {'received':t,'sent':t-duration/1000,'duration_ms':duration,'status':status,'error':error,'served_mode':mode}

def event(t,bad=False):return {'time':t,'causes':['http_error'] if bad else []}

def policy(limit=None):return {'latency_limit_ms':limit,'recovery_seconds':10,'freshness_seconds':10,'scope':'current_settings_view'}

def run(ended=None):return {'id':'case','status':'stopped' if ended is not None else 'running','ended':ended}

class StatusTests(unittest.TestCase):
    def test_no_time_limit_is_not_a_claim_of_healthy_latency(self):
        slow=request_row(10,1500)
        self.assertIsNone(response_cause(slow,None))
        self.assertEqual(response_cause(slow,1000),'slow_response')
        self.assertIsNone(response_cause(request_row(10,1000),1000))
        self.assertEqual(response_cause(request_row(10,status=503),None),'http_error')
        self.assertEqual(response_cause(request_row(10,status=None,error='TimeoutError'),1000),'no_response')
        value=build_status(run(),[slow],[],policy(),11)
        self.assertEqual(value['response_state'],'responses_received_no_limit')
        self.assertEqual(value['response_incidents'],[])
        self.assertIsNone(value['model_last_decision_time'])

    def test_policy_persistence_validation_and_invalid_update_keeps_previous(self):
        parent=ROOT/'work/status-test-tmp';parent.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=parent) as tmp:
            p=Path(tmp)/'policy.json';s=ServicePolicy(p)
            self.assertIsNone(s.get()['latency_limit_ms']);self.assertFalse(p.exists())
            s.set({'latency_limit_ms':1000})
            original=p.read_bytes()
            for bad in (True,0,-1,float('nan'),float('inf'),'1000',[],10**1000):
                with self.assertRaises(ValueError):s.set({'latency_limit_ms':bad})
                self.assertEqual(p.read_bytes(),original)
            with self.assertRaises(ValueError):s.set({'latency_limit_ms':10,'other':1})
            self.assertEqual(ServicePolicy(p).get()['latency_limit_ms'],1000)
            s.set({'latency_limit_ms':None});self.assertIsNone(ServicePolicy(p).get()['latency_limit_ms'])

    def test_repeated_signals_merge_and_new_episode_after_confirmed_recovery(self):
        events=[event(0,True),event(1,True)]+[event(t) for t in range(2,13)]+[event(14,True)]
        episodes=group_events(events)
        self.assertEqual(len(episodes),2)
        self.assertEqual(episodes[0]['signals'],2)
        self.assertEqual(episodes[0]['status'],'resolved')
        self.assertEqual(episodes[0]['resolved_at'],12)
        self.assertEqual(episodes[1]['status'],'active')

    def test_missing_observations_and_stopping_do_not_confirm_recovery(self):
        result=group_events([event(0,True),event(1),event(30)])
        self.assertEqual(result[0]['status'],'recovering')
        self.assertEqual(result[0]['normal_since'],30)
        result=group_events([event(0,True)],ended=100)
        self.assertEqual(result[0]['status'],'observation_ended')
        self.assertIsNone(result[0]['resolved_at'])
        self.assertEqual(group_events([event(0,True)])[0]['status'],'active')

    def test_bad_response_during_recovery_resets_waiting_interval(self):
        events=[event(0,True)]+[event(t) for t in range(1,10)]+[event(10,True)]+[event(t) for t in range(11,21)]
        result=group_events(events)
        self.assertEqual(len(result),1);self.assertEqual(result[0]['status'],'recovering')
        result=group_events(events+[event(21)])
        self.assertEqual(result[0]['resolved_at'],21)

    def test_model_batch_groups_do_not_count_false_rows_as_recovery_same_packet(self):
        def decision(source,issued,alarm):
            return {'ts':source,'result':json.dumps({'decision_source_time':source,'emitted_at':f'2026-01-01T00:00:{issued:02d}+00:00','alarm':alarm})}
        rows=[decision(16,10,False),decision(16,10,True),decision(16,10,True),decision(32,30,False),decision(48,50,False)]
        events=model_events(rows)
        self.assertEqual(len(events),3);self.assertEqual(events[0]['count'],2)
        result=group_events(events,max_gap=48)
        self.assertEqual(result[0]['signals'],2);self.assertEqual(result[0]['status'],'resolved')

    def test_status_uses_actual_outcomes_not_control_labels(self):
        a=[request_row(1,status=503),request_row(2,status=503)]+[request_row(t) for t in range(3,15)]
        b=[dict(r,served_mode='different-label',control_mode='delay') for r in a]
        first=build_status(run(),a,[],policy(1000),14)
        second=build_status(run(),b,[],policy(1000),14)
        self.assertEqual(first,second)
        self.assertEqual(len(first['response_incidents']),1)
        self.assertEqual(first['response_incidents'][0]['status'],'resolved')
        self.assertEqual(first['response_state'],'within_limit')

    def test_history_is_anchored_to_end_and_stale_live_status_unknown(self):
        rows=[request_row(10,status=None,error='TimeoutError')]
        value=build_status(run(ended=11),rows,[],policy(),100000)
        self.assertTrue(value['observation_ended']);self.assertEqual(value['response_state'],'no_response')
        self.assertEqual(value['response_incidents'][0]['status'],'observation_ended')
        value=build_status(run(),rows,[],policy(),30)
        self.assertEqual(value['response_state'],'no_recent_data')
        self.assertEqual(value['response_incidents'][0]['status'],'active')

    def test_http_routes_and_fast_scenario_without_waiting_for_training(self):
        parent=ROOT/'work/status-test-tmp';parent.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=parent) as tmp:
            store=Store(Path(tmp)/'monitor.sqlite3');server=Server(('127.0.0.1',0),store)
            thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            base=f'http://127.0.0.1:{server.server_port}'
            def post(path,data):
                import urllib.request
                req=urllib.request.Request(base+path,data=json.dumps(data).encode(),headers={'Content-Type':'application/json'})
                with open_direct(req) as response:return json.load(response)
            try:
                self.assertIsNone(get_json(base+'/api/bench/status'))
                store.register({'id':'case','features':['cpu','memory']})
                journal=server.bench.journal;journal.start('case','case',{'scope':'fabricated software fixture'})
                start=time.time()-30
                for t in range(20):journal.request('case',start+t-.1,start+t,1500 if t<3 else 50,503 if t==1 else 200,None,'normal')
                with journal.db() as db:db.execute('UPDATE runs SET ended=?,status=? WHERE id=?',(start+20,'stopped','case'))
                before=store.state('case')
                post('/api/bench/settings',{'latency_limit_ms':1000})
                value=get_json(base+'/api/bench/status?run=case')
                self.assertEqual(len(value['response_incidents']),1)
                self.assertEqual(value['response_incidents'][0]['signals'],3)
                self.assertEqual(value['response_incidents'][0]['status'],'resolved')
                self.assertEqual(value['model_incidents'],[])
                self.assertEqual(store.state('case'),before)
                self.assertEqual(store.state('case')[0]['samples'],0)
                with open_direct(base+'/status.js') as response:self.assertIn('ThesisServiceView',response.read().decode())
                with self.assertRaises(urllib.error.HTTPError) as bad:post('/api/bench/settings',{'latency_limit_ms':True})
                self.assertEqual(bad.exception.code,400);bad.exception.close()
                with self.assertRaises(urllib.error.HTTPError) as missing:get_json(base+'/api/bench/status?run=unknown')
                self.assertEqual(missing.exception.code,404);missing.exception.close()
                self.assertEqual(server.bench.policy.get()['latency_limit_ms'],1000)
            finally:server.close();thread.join(timeout=5)

if __name__=='__main__':unittest.main(verbosity=2)
