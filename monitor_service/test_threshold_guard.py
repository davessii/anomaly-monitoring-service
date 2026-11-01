"""Deterministic SOFTWARE fixtures only; no application/network or new data."""
import copy
import json
import math
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from .threshold_guard import Limits,GuardComparison,save_outputs,verify_outputs
from .threshold_guard_protocol import NAMES,load_protocol
from .threshold_guard_schedule import actions,GuardSchedule
from .threshold_guard_evaluation import summarize,ordinary_duration,verify_series,evaluate_series,screening,build_series_gate
from .threshold_guard_measurements import checks
from .threshold_guard_measurements import segments
from .test_cached_cycle_collection import workspace_fixture


class StubCore:
    def __init__(self):self.simple=SimpleNamespace(seen=895,cuts={'cpu_per_request':100.,'expected_cpu':200.})
    def observe(self,raw):
        self.simple.seen+=1;post=self.simple.seen-896
        if post==0:return None
        scores={'cycles_per_request':500.,'expected_cycles':1000.,'separate_limits':2.}
        cuts={'cycles_per_request':self.simple.cuts['cpu_per_request'],'expected_cycles':self.simple.cuts['expected_cpu'],'separate_limits':3.}
        ds={name:dict(score=s,threshold=cuts[name],alarm=s>cuts[name],own_score_seconds=0.) for name,s in scores.items()}
        ds['ipca']=dict(alarm=False,base_alarm=False,q_score=.5,auxiliary_score=.5,auxiliary_threshold=1.,own_compute_seconds=0.)
        if post%16==0:self.simple.cuts={'cpu_per_request':1000.,'expected_cpu':2000.}
        return dict(timestamp=float(post),decisions=ds,common_input_preparation_seconds=0.,common_window_preparation_seconds=0.,simple_update_seconds={k:0. for k in scores})


class PolicyTests(unittest.TestCase):
    def test_additive_step_is_based_on_initial_not_current(self):
        p=Limits(100)
        for expected in (110,120,130):p.advance(1000);self.assertEqual(p.limited,expected)
        p.advance(80);self.assertEqual(p.limited,80)
        p.advance(1000);self.assertEqual(p.limited,90)
        self.assertEqual(p.initial,100)

    def test_bad_initial_and_candidate_fail_without_substitutes(self):
        for x in (0,-1,math.inf,math.nan,False):
            with self.assertRaises(ValueError):Limits(x)
        for x in (-1,math.inf,math.nan):
            with self.assertRaises(ValueError):Limits(100).advance(x)

    def test_boundary_row_uses_old_threshold_and_next_uses_updated(self):
        core=StubCore();engine=GuardComparison(core);self.assertIsNone(engine.observe({}))
        result=None
        for _ in range(16):result=engine.observe({})
        self.assertTrue(result['block_updated'])
        self.assertEqual(result['decisions']['ratio_updated']['threshold'],100)
        self.assertEqual(result['decisions']['ratio_limited']['threshold'],100)
        result=engine.observe({})
        self.assertEqual(result['decisions']['ratio_fixed']['threshold'],100)
        self.assertEqual(result['decisions']['ratio_updated']['threshold'],1000)
        self.assertEqual(result['decisions']['ratio_limited']['threshold'],110)
        self.assertFalse(result['decisions']['ratio_updated']['alarm'])
        self.assertTrue(result['decisions']['ratio_limited']['alarm'])
        self.assertEqual({result['decisions']['ratio_'+v]['score'] for v in ('fixed','updated','limited')},{500.})
        self.assertEqual(core.simple.seen,913)

    def test_outputs_do_not_modify_the_common_history_or_reference_flags(self):
        core=StubCore();engine=GuardComparison(core);engine.observe({});r=engine.observe({})
        r['decisions']['ratio_limited']['alarm']=False
        self.assertTrue(r['common']['decisions']['cycles_per_request']['alarm'])
        self.assertFalse(r['decisions']['ipca_reference']['alarm'])
        self.assertEqual(core.simple.cuts['cpu_per_request'],100)


class ScheduleTests(unittest.TestCase):
    def test_same_load_and_fixed_event_slots_different_durations(self):
        for scenario,durations in (('A',[30,10]),('B',[10,30]),('C',[])):
            seq=actions(scenario)
            self.assertEqual([(a.at,a.value) for a in seq if a.kind=='load'],[(60,8),(90,2),(120,4),(150,2)])
            events=[a for a in seq if a.kind=='extra_cpu']
            self.assertEqual([a.seconds for a in events],durations)
            self.assertEqual([a.at for a in events],[210,300] if events else [])
            self.assertEqual(seq[-1].at,390)
        self.assertEqual(load_protocol()['collection']['planned_order'],['A','B','C','B','C','A','C','A','B'])

    def test_exact_warmup_transition_and_late_command_stop(self):
        s=GuardSchedule('C')
        for n in range(0,896,32):s.tick(n,'warmup',float(n))
        self.assertEqual(s.tick(896,'monitoring',896.)[0].at,0)
        with self.assertRaises(ValueError):s.tick(958,'monitoring',958.)
        with self.assertRaises(ValueError):GuardSchedule('unknown')

    def test_complete_C_log_and_late_or_missing_command_rejected(self):
        from dataclasses import asdict
        log=[]
        for n in range(0,896,32):
            t=1000+n+.1
            log.append(dict(action=dict(at=float(n),kind='load',value=(2,4,8)[n//32%3],seconds=120),samples=n,
                requested_wall_time=t,completed_wall_time=t+.01,post_elapsed_seconds=None))
        for a in actions('C'):
            t=1896+a.at+.1
            log.append(dict(action=asdict(a),samples=896+int(a.at),requested_wall_time=t,completed_wall_time=t+.01,post_elapsed_seconds=a.at))
        with workspace_fixture() as folder:
            (folder/'schedule.json').write_text(json.dumps(dict(scenario='C')),encoding='utf-8')
            path=folder/'schedule_log.jsonl'
            def write(records):path.write_text(''.join(json.dumps(r)+'\n' for r in records),encoding='utf-8')
            write(log);scenario,parts,_=segments(folder,2286.)
            self.assertEqual(scenario,'C');self.assertFalse(any(s['mode']=='extra_cpu' for s in parts))
            bad=copy.deepcopy(log);bad[-1]['post_elapsed_seconds']+=2;write(bad)
            with self.assertRaises(ValueError):segments(folder,2286.)
            write(log[:-1])
            with self.assertRaises(ValueError):segments(folder,2286.)


def raw_fixture(n):
    return dict(metrics_schema='application_cycles_snapshot_v1',cpu_cycles_status='available',
        cpu_cycles_source='QueryProcessCycleTime',cpu_cycles_unit='cycles',timestamp=1000.+n,
        cpu_cycles=1000000*n,cpu_cycles_after_snapshot=1000000*n+100,
        cpu_cycles_read_span_ns=1000,requests_completed=2*n,active_requests=0,memory_mib=30.,collector_received_at=1000.+n+.001)


class StubEmitter:
    def __init__(self):self.n=0
    def observe(self,row):
        self.n+=1
        if self.n<=897:return None
        return dict(timestamp=row['timestamp'],decisions={name:dict(alarm=False,availability_cost_seconds=0.) for name in NAMES})
    def state(self):return {}


class OutputTests(unittest.TestCase):
    def test_complete_barrier_tamper_and_fresh_folder_only(self):
        with workspace_fixture() as root,patch('monitor_service.threshold_guard.GuardComparison',StubEmitter):
            folder=root/'outputs';barrier=save_outputs((raw_fixture(i) for i in range(899)),folder)
            self.assertEqual(barrier['post_rows'],2);self.assertEqual(len(barrier['files']),13)
            verify_outputs(folder)
            with self.assertRaises(FileExistsError):save_outputs([],folder)
            with (folder/'ratio_limited.jsonl').open('a') as stream:stream.write('{}\n')
            with self.assertRaises(ValueError):verify_outputs(folder)

    def test_invalid_arrival_and_incomplete_input_leave_no_complete_marker(self):
        with workspace_fixture() as root,patch('monitor_service.threshold_guard.GuardComparison',StubEmitter):
            folder=root/'failed';bad=raw_fixture(1);bad['collector_received_at']=0
            with self.assertRaises(ValueError):save_outputs([raw_fixture(0),bad],folder)
            self.assertFalse((folder/'eight_outputs_complete.json').exists())
            with self.assertRaises(ValueError):save_outputs([raw_fixture(0)],root/'short')
            self.assertFalse((root/'short/eight_outputs_complete.json').exists())


class EvaluationTests(unittest.TestCase):
    def test_normal_only_counts_duration_and_zero_events(self):
        records=[dict(timestamp=float(t),alarm=t<3,available_at=float(t)) for t in range(6)]
        parts=[dict(start=0.,end=5.,category='baseline',mode='normal',rate=2)]
        r=summarize(records,[],parts)
        self.assertEqual(r['events'],[]);self.assertEqual(r['ordinary_notification_count'],1)
        self.assertEqual(r['ordinary_alarm_seconds'],3.)

    def test_events_gaps_and_unknown_do_not_inflate_ordinary_duration(self):
        parts=[dict(start=0.,end=20.,category='baseline',mode='normal',rate=2)]
        records=[dict(timestamp=t,alarm=a,available_at=t) for t,a in [(0.,True),(2.,True),(4.,None),(10.,True),(12.,False)]]
        self.assertEqual(ordinary_duration(records,parts,[dict(start=1.,end=3.)]),4.)

    def test_nine_record_gate_blocks_outcome_read_on_missing_or_wrong_series(self):
        with patch('monitor_service.threshold_guard_evaluation.legacy.load_outcomes') as outcomes:
            with self.assertRaises(ValueError):evaluate_series([],Path('unused'),Path('unused'))
            outcomes.assert_not_called()
            exported={'collection_seal_sha256':'x'};admission={'scenario':'C','run':'x'}
            with patch('monitor_service.threshold_guard_evaluation.measurements.admitted',return_value=(exported,admission)),patch('monitor_service.threshold_guard_evaluation.verify_outputs',return_value={}):
                with self.assertRaises(ValueError):verify_series([dict(processing='unused',outputs='unused')]*9)
            outcomes.assert_not_called()

    def test_ten_second_recovery_rule_is_unchanged(self):
        parts=[dict(start=0.,end=30.,category='baseline',mode='normal',rate=2)]
        records=[dict(timestamp=float(t),alarm=t==0 or t>=3,available_at=float(t)) for t in range(8)]
        r=summarize(records,[dict(start=3.,end=7.)],parts)
        self.assertFalse(r['events'][0]['detected_before_end'])
        self.assertEqual(r['point_alarm_count'],6)

    def test_beating_updated_is_insufficient_when_fixed_is_as_good(self):
        def method(flags):
            return dict(events=[dict(detected_before_end=x) for x in flags],ordinary_notification_count=0,ordinary_alarm_seconds=0.)
        records=[]
        for scenario in load_protocol()['collection']['planned_order']:
            g=method([True,True] if scenario!='C' else [])
            u=method([True,False] if scenario=='A' else [True,True] if scenario=='B' else [])
            records.append(dict(scenario=scenario,methods=dict(ratio_limited=g,ratio_updated=u,ratio_fixed=copy.deepcopy(g))))
        result=screening(records,'ratio');self.assertTrue(result['updated_comparison_passes']);self.assertFalse(result['passes'])
        records[2]['methods']['ratio_fixed']['ordinary_notification_count']=1
        self.assertTrue(screening(records,'ratio')['passes'])
        records[1]['methods']['ratio_limited']['ordinary_alarm_seconds']=1.
        self.assertFalse(screening(records,'ratio')['passes'])

    def test_complete_synthetic_series_gate_then_C_quality_and_tamper_block(self):
        # Synthetic file fixtures and mocked measurement admissions, not new data.
        with workspace_fixture() as root,patch('monitor_service.threshold_guard.GuardComparison',StubEmitter):
            raw=[raw_fixture(i) for i in range(899)];scores=root/'scores';save_outputs(raw,scores)
            entries=[];trusted={}
            for i,scenario in enumerate(load_protocol()['collection']['planned_order']):
                p=root/str(i);p.mkdir()
                (p/'measurements.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in raw),encoding='utf-8')
                (p/'export_complete.json').write_text(json.dumps(dict(id=i)),encoding='utf-8')
                (p/'guard_measurement_admission.json').write_text(json.dumps(dict(id=i)),encoding='utf-8')
                trusted[str(p)]=(dict(collection=str(p),collection_seal_sha256=str(i),run=str(i)),dict(scenario=scenario,run=str(i)))
                entries.append(dict(processing=str(p),outputs=str(scores)))
            def admission(p):return trusted[str(p)]
            def parts(p,last):
                scenario=trusted[str(p)][1]['scenario']
                normal=dict(start=1896.,end=1899.,mode='normal',rate=2,category='baseline')
                events=[] if scenario=='C' else [dict(start=1897.,end=1897.2,requested=1897.,mode='extra_cpu',category='event',rate=2),dict(start=1898.,end=1898.2,requested=1898.,mode='extra_cpu',category='event',rate=2)]
                return scenario,[normal]+events,[]
            requests=[dict(id=i,sent=t,received=t+.1,iterations=32,served_mode='extra_cpu') for i,t in enumerate((1897.,1898.))]
            with patch('monitor_service.threshold_guard_evaluation.measurements.admitted',side_effect=admission),patch('monitor_service.threshold_guard_evaluation.measurements.segments',side_effect=parts),patch('monitor_service.threshold_guard_evaluation.legacy.load_outcomes',return_value=requests) as outcomes:
                gate=root/'gate.json';build_series_gate(entries,gate);outcomes.assert_not_called()
                report=evaluate_series(entries,gate,root/'quality.json')
                self.assertEqual(outcomes.call_count,9);self.assertEqual(report['records'][2]['events'],[])
                self.assertFalse(report['primary']['passes'])
                outcomes.reset_mock()
                (Path(entries[0]['processing'])/'guard_measurement_admission.json').write_text('{}',encoding='utf-8')
                with self.assertRaises(ValueError):evaluate_series(entries,gate,root/'invalid.json')
                outcomes.assert_not_called();self.assertFalse((root/'invalid.json').exists())


def measurement_fixture():
    rows=[];requests=[];receipts=[];total=0;cycles=0
    for i in range(961):
        rate=(2,8,2)[max(0,(i-865)//32)] if i>=865 else 2
        row=raw_fixture(i)
        if i:
            for j in range(rate):
                sent=1000+i-.8+j*.6/rate;received=sent+.01
                r=dict(sent=sent,received=received,status=200,error=None,served_mode='normal',iterations=0)
                requests.append(r);receipts.append(dict(run='fixture',sent=sent,received=received,status=200,error=None,body_sha256='f'*64))
            total+=rate;cycles+=4_000_000+rate*2_000_000
        row.update(cpu_cycles=cycles,cpu_cycles_after_snapshot=cycles+100,requests_completed=total,
            client_target_rps=rate,client_schedule_attempts=total,client_submitted_requests=total,client_skipped_slots=0)
        rows.append(row)
    parts=[dict(start=1864.,end=1896.,mode='normal',rate=2,category='baseline'),
           dict(start=1896.,end=1928.,mode='normal',rate=8,category='increased_load'),
           dict(start=1928.,end=1960.,mode='normal',rate=2,category='return_to_normal')]
    closed=dict(run='fixture',scheduled=total,submitted=total,skipped=0,all_submitted_results_persisted=True,
                persistence=dict(complete=True),http_client=dict(accounting_complete=True))
    return rows,requests,receipts,closed,parts,[dict(samples=896)]


class MeasurementTests(unittest.TestCase):
    def test_C_accepts_no_events_but_A_requires_two(self):
        args=measurement_fixture();result=checks(*args,0)
        self.assertTrue(result['admitted'],result['gates'])
        self.assertFalse(checks(*args,2)['gates']['planned_extra_events_executed'])

    def test_C_rejects_unplanned_extra_work_and_skips(self):
        args=measurement_fixture();args[1][0].update(served_mode='extra_cpu',iterations=32)
        self.assertFalse(checks(*args,0)['gates']['normal_only_no_extra_requests'])
        args=measurement_fixture();args[3]['skipped']=1
        self.assertFalse(checks(*args,0)['gates']['no_skips_and_complete_request_accounting'])


if __name__=='__main__':unittest.main(verbosity=2)
