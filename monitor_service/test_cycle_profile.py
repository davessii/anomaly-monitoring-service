"""Technical deterministic fixtures. No empirical detection comparison."""
import copy
from contextlib import redirect_stdout
from io import StringIO
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch
from .cycle_profile import (CycleComparison,CycleWindow,CycleBench,FEATURES,PROFILE,
                            detector_values,project)
from .cycle_outputs import save_outputs,verify_outputs,NAMES
from .immediate import ImmediateDetector
from .efficiency_pilot import main
from .store import Store

ROOT=Path(__file__).resolve().parents[1]


def snapshots(count):
    cycles=2**54+12345;completed=0
    for i in range(count):
        if i:
            n=(2,4,8)[((i-1)//32)%3]
            completed+=n
            cycles+=4_000_000+2_000_000*n+(i%7)*1200
        yield {'timestamp':1000+i,'memory_mib':30+(i%13)*.01,'active_requests':i%2,
               'requests_completed':completed,'cpu_cycles':cycles,'cpu_cycles_after_snapshot':cycles+2000+i%3,
               'cpu_cycles_read_span_ns':25000,'cpu_cycles_status':'available','cpu_cycles_error':None,
               'metrics_schema':'application_cycles_snapshot_v1','cpu_cycles_unit':'cycles',
               'cpu_cycles_source':'QueryProcessCycleTime','collector_received_at':1000+i+.001}


def numeric_record(record):
    return {k:v for k,v in record.items() if k not in ('own_score_seconds','own_compute_seconds')}


class CycleProfileTests(unittest.TestCase):
    def test_large_integer_differences_before_float_conversion(self):
        a,b=list(snapshots(2))
        value=detector_values(a,b)
        expected=((b['cpu_cycles']-a['cpu_cycles_after_snapshot'])+
                  (b['cpu_cycles_after_snapshot']-a['cpu_cycles']))/2
        self.assertEqual(value[FEATURES[0]],expected)
        self.assertEqual(value['completed_per_second'],2)
        self.assertNotIn('app_cpu_percent',value)

    def test_cycle_window_common_endpoints_units_and_alignment(self):
        window=CycleWindow();rows=list(snapshots(11))
        for row in rows:result=window.observe(row)
        expected=((rows[-1]['cpu_cycles']-rows[0]['cpu_cycles_after_snapshot'])+
                  (rows[-1]['cpu_cycles_after_snapshot']-rows[0]['cpu_cycles']))/2
        self.assertEqual(result['resource_cycles'],expected)
        self.assertEqual(result['completed'],20)
        self.assertEqual(result['features'][0],expected/10)
        self.assertEqual(result['resource_unit'],'cycles')
        self.assertTrue(result['alignment_screen_passed'])
        self.assertNotIn('cpu_ms',result)

    def test_no_seconds_fallback_and_raw_errors_do_not_change_state(self):
        rows=list(snapshots(21));engine=CycleComparison()
        for row in rows[:20]:engine.observe(row)
        state=json.dumps(engine.to_state(),sort_keys=True,allow_nan=False)
        changes=[{'cpu_cycles_status':'error','cpu_seconds':10**9},
                 {'cpu_cycles':None},{'cpu_cycles_unit':'milliseconds'},
                 {'cpu_cycles_source':'another'}, {'timestamp':rows[19]['timestamp']},
                 {'timestamp':rows[19]['timestamp']+4},
                 {'requests_completed':0}, {'cpu_cycles':True},
                 {'cpu_cycles':rows[19]['cpu_cycles']-100}]
        for change in changes:
            with self.assertRaises((ValueError,KeyError)):engine.observe(dict(rows[20],**change))
            self.assertEqual(state,json.dumps(engine.to_state(),sort_keys=True,allow_nan=False))

    def test_wide_read_alignment_is_a_diagnostic_not_a_fault_label(self):
        window=CycleWindow()
        for row in snapshots(11):
            row['cpu_cycles_after_snapshot']=row['cpu_cycles']+5_000_000
            result=window.observe(row)
        self.assertTrue(result['valid'])
        self.assertFalse(result['alignment_screen_passed'])
        self.assertNotIn('alarm',result)

    def test_four_outputs_same_progress_and_unchanged_ipca_math(self):
        engine=CycleComparison();reference=ImmediateDetector(FEATURES,components=2)
        previous=None;post=0
        for row in snapshots(930):
            result=engine.observe(row)
            if previous is not None:
                values=detector_values(previous,row)
                expected=reference.observe(row['timestamp'],[values[f] for f in FEATURES])
                if expected:
                    post+=1
                    self.assertEqual(set(result['decisions']),set(NAMES))
                    actual=numeric_record(result['decisions']['ipca'])
                    actual.pop('input_resource_unit')
                    self.assertEqual(actual,expected[0])
                    self.assertEqual(result['decisions']['cycles_per_request']['score'],
                                     result['window']['resource_cycles']/result['window']['completed'])
                    self.assertEqual(result['decisions']['cycles_per_request']['score_unit'],'cycles_per_completed_request')
                else:self.assertIsNone(result)
            previous=row
        self.assertEqual(post,33)
        self.assertEqual(engine.simple.seen,reference.seen)
        self.assertEqual(engine.simple.fit_diagnostics['resource_unit'],'cycles')
        self.assertNotIn('cpu_intercept_ms',engine.simple.fit_diagnostics)

    def test_current_block_cannot_set_its_own_simple_thresholds(self):
        rows=list(snapshots(914));engine=CycleComparison()
        for row in rows[:897]:engine.observe(row)
        old=dict(engine.simple.cuts)
        for offset,row in enumerate(rows[897:913]):
            altered=dict(row)
            altered['cpu_cycles']+=(offset+1)*2_000_000_000
            altered['cpu_cycles_after_snapshot']+=(offset+1)*2_000_000_000
            result=engine.observe(altered)
            for old_name,new in (('cpu_per_request','cycles_per_request'),('expected_cpu','expected_cycles'),('separate_limits','separate_limits')):
                self.assertEqual(result['decisions'][new]['threshold'],old[old_name])
        self.assertNotEqual(engine.simple.cuts,old)

    def test_json_restore_at_initial_fit_calibration_and_partial_block(self):
        rows=list(snapshots(931));engine=CycleComparison()
        checkpoints={0,128,256,384,896,897,913}
        for i,row in enumerate(rows):
            result=engine.observe(row)
            if i in checkpoints:
                state=json.loads(json.dumps(engine.to_state(),allow_nan=False))
                restored=CycleComparison.from_state(state)
                self.assertEqual(engine.to_state(),restored.to_state())
                if i+1<len(rows):
                    expected=CycleComparison.from_state(state).observe(rows[i+1])
                    actual=restored.observe(rows[i+1])
                    if expected:
                        self.assertEqual({m:numeric_record(d) for m,d in actual['decisions'].items()},
                                         {m:numeric_record(d) for m,d in expected['decisions'].items()})

    def test_wrong_profile_oversized_history_and_nonfinite_state_rejected(self):
        engine=CycleComparison()
        for row in snapshots(899):engine.observe(row)
        for change in ('profile','feature','history','pending','nonfinite'):
            state=engine.to_state()
            if change=='profile':state['profile']='cpu_percent_v1'
            if change=='feature':state['ipca']['features'][0]='app_cpu_percent'
            if change=='history':state['simple']['histories']['cpu_per_request']=[1]*513
            if change=='pending':state['simple']['pending']=[]
            if change=='nonfinite':state['simple']['fitted']['scale'][0]=float('nan')
            with self.assertRaises(ValueError):CycleComparison.from_state(state)

    def test_metadata_and_old_cpu_seconds_do_not_affect_any_method(self):
        a,b=CycleComparison(),CycleComparison()
        for row in snapshots(915):
            left=a.observe(row)
            right=b.observe(dict(row,cpu_seconds=10**9,extra_work_iterations=10**12,
                                 control_mode='extra_cpu',served_mode='error',http_status=503))
            if left:
                self.assertEqual({m:numeric_record(v) for m,v in left['decisions'].items()},
                                 {m:numeric_record(v) for m,v in right['decisions'].items()})

    def test_save_four_outputs_barrier_and_tamper_detection(self):
        with tempfile.TemporaryDirectory() as temporary:
            output=Path(temporary)/'new';barrier=save_outputs(snapshots(914),output)
            self.assertEqual(barrier['post_warmup_rows'],17)
            self.assertEqual(verify_outputs(output),barrier)
            for name in NAMES:
                records=[json.loads(line) for line in (output/(name+'.jsonl')).read_text(encoding='utf-8').splitlines()]
                self.assertEqual(len(records),17)
                self.assertEqual(records[0]['profile'],PROFILE)
            self.assertFalse((output/'quality.json').exists())
            with (output/'ipca.jsonl').open('a',encoding='utf-8') as stream:stream.write('{}\n')
            with self.assertRaises(ValueError):verify_outputs(output)

    def test_insufficient_or_corrupt_input_has_no_complete_barrier(self):
        for variant in ('short','bad'):
            with tempfile.TemporaryDirectory() as temporary:
                output=Path(temporary)/'new';rows=list(snapshots(10 if variant=='short' else 899))
                if variant=='bad':rows[-1]['cpu_cycles_status']='error'
                with self.assertRaises(ValueError):save_outputs(rows,output)
                self.assertFalse((output/'four_outputs_complete.json').exists())
                self.assertEqual(json.loads((output/'scoring_status.json').read_text())['status'],'failed')
                with self.assertRaises(FileExistsError):save_outputs(snapshots(914),output)

    def test_cycles_preview_does_not_start_collection(self):
        with patch('monitor_service.efficiency_pilot.collect') as collect,redirect_stdout(StringIO()) as stream:
            main(['--profile','cycles_v1','--order','2'])
        collect.assert_not_called()
        self.assertEqual(json.loads(stream.getvalue())['input_profile'],PROFILE)

    def test_cycle_bench_real_anchor_inputs_journal_and_stop(self):
        parent=ROOT/'work/cycle-profile-check-v01/live';parent.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(dir=parent) as directory:
            store=Store(Path(directory)/'monitor.sqlite3');bench=CycleBench(store)
            process=None
            try:
                run=bench.start();process=bench.process
                deadline=time.monotonic()+8
                while time.monotonic()<deadline:
                    with bench.journal.db() as db:rows=list(db.execute('SELECT counters,detector_values FROM samples ORDER BY id'))
                    if len(rows)>=2:break
                    time.sleep(.05)
                self.assertGreaterEqual(len(rows),2)
                anchor=json.loads((bench.root/(run['id']+'-cycle-anchor.json')).read_text(encoding='utf-8'))
                self.assertEqual(anchor['profile'],PROFILE)
                previous=anchor['counters']
                for record in rows:
                    raw=json.loads(record['counters']);values=json.loads(record['detector_values'])
                    self.assertEqual(values,detector_values(previous,raw))
                    self.assertGreaterEqual(raw['collector_received_at'],raw['timestamp'])
                    previous=raw
                state=store.state(run['host'])[0]
                self.assertEqual(state['features'],list(FEATURES))
                self.assertEqual(state['phase'],'warmup')
                self.assertEqual(bench.journal.runs()[0]['config']['input_profile'],PROFILE)
                bench.stop();bench.thread.join(timeout=12)
                self.assertFalse(bench.thread.is_alive());self.assertIsNotNone(process.poll())
                evidence={'scope':'Short live cycle-profile transport test, no fitted model or detection evaluation.',
                          'features':state['features'],'phase':state['phase'],'anchor':anchor,
                          'saved_counters':[json.loads(r['counters']) for r in rows],
                          'saved_values':[json.loads(r['detector_values']) for r in rows],
                          'child_exited':process.poll() is not None,'collector_exited':not bench.thread.is_alive()}
                (parent.parent/'live_evidence.json').write_text(json.dumps(evidence,ensure_ascii=False,indent=2),encoding='utf-8')
            finally:bench.close()


if __name__=='__main__':unittest.main(verbosity=2)
