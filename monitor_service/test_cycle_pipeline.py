"""Deterministic software fixtures; no empirical run or collection of training data."""
import copy
from contextlib import closing
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch
from .bench_journal import Journal
from .cycle_collection import seal_collection, RecordedCycleBench
from .cycle_outputs import sha
from .cycle_profile import PROFILE,FEATURES,detector_values
from .efficiency_pilot import code_snapshot
from .efficiency_schedule import preview,post_schedule
from . import cycle_pipeline as pipeline


def put(path,value):path.write_text(json.dumps(value,allow_nan=False),encoding='utf-8')


def fixture(folder,wide=False):
    folder=Path(folder);folder.mkdir();(folder/'testbench').mkdir();run='fixture-only'
    config=preview(1);config['input_profile']=PROFILE;put(folder/'schedule.json',config)
    actions=[]
    for count in range(0,896,32):
        t=1000+count+.1
        actions.append(dict(action={'at':float(count),'kind':'load','value':(2,4,8)[(count//32)%3],'seconds':120},
            samples=count,requested_wall_time=t,completed_wall_time=t+.01,post_elapsed_seconds=None,result={}))
    for action in post_schedule(1):
        t=1896+action.at+.1
        actions.append(dict(action=asdict(action),samples=896+int(action.at),requested_wall_time=t,
                            completed_wall_time=t+.01,post_elapsed_seconds=action.at,result={}))
    (folder/'schedule_log.jsonl').write_text(''.join(json.dumps(a)+'\n' for a in actions),encoding='utf-8')
    journal=Journal(folder/'application_snapshot.sqlite3')
    previous={'timestamp':1000.,'memory_mib':30.,'active_requests':0,'requests_completed':0,'cpu_cycles':2**54,
        'cpu_cycles_after_snapshot':2**54+(6_000_000 if wide else 2000),'cpu_cycles_read_span_ns':20000,
        'metrics_schema':'application_cycles_snapshot_v1','cpu_cycles_status':'available','cpu_cycles_error':None,
        'cpu_cycles_unit':'cycles','cpu_cycles_source':'QueryProcessCycleTime'}
    put(folder/f'testbench/{run}-cycle-anchor.json',dict(profile=PROFILE,role='raw_anchor_not_a_training_row',counters=previous))
    receipts=[];requests=[];samples=[];total=0;cycles=previous['cpu_cycles']
    for i in range(1,1287):
        t=1000+i
        command=next(a for a in reversed(actions) if a['requested_wall_time']<=t-.5)
        action=command['action'];mode='extra_cpu' if action['kind']=='extra_cpu' else 'normal'
        n=int(action['value']) if action['kind']=='load' else 2
        for j in range(n):
            sent=t-1+.2+(j+.2)*.6/n;received=sent+(.04 if mode=='extra_cpu' else .01)
            request_id=len(requests)+1
            requests.append((request_id,run,sent,received,(received-sent)*1000,200,None,mode))
            receipts.append(dict(run=run,sent=sent,received=received,status=200,error=None,body_sha256='f'*64))
        total+=n;cycles+=4_000_000+n*2_000_000+(i%7)*1200+(160_000_000 if mode=='extra_cpu' else 0)
        raw=dict(previous,timestamp=float(t),cpu_cycles=cycles,cpu_cycles_after_snapshot=cycles+(6_000_000 if wide else 2000),
                 memory_mib=30+(i%13)*.01,active_requests=i%2,requests_completed=total,
                 collector_received_at=t+.002,collector_received_monotonic_ns=i*10**9,
                 client_target_rps=n,client_schedule_attempts=total,client_submitted_requests=total,client_skipped_slots=0)
        values=detector_values(previous,raw)
        samples.append((run,t,json.dumps(raw),json.dumps(values),t+.01,int(i>896)));previous=raw
    with journal.db() as db:
        db.execute('INSERT INTO runs VALUES(?,?,?,?,?,?,?)',(run,run,1000,2286.2,'stopped',None,json.dumps(dict(input_profile=PROFILE,features=list(FEATURES)))))
        db.executemany('INSERT INTO requests VALUES(?,?,?,?,?,?,?,?)',requests)
        db.executemany('INSERT INTO request_work VALUES(?,?,?,?)',[(r[0],31.25 if r[-1]=='extra_cpu' else 0,32. if r[-1]=='extra_cpu' else 0,2048 if r[-1]=='extra_cpu' else 0) for r in requests])
        db.executemany('INSERT INTO samples(run,ts,counters,detector_values,ingest_returned,decision_count) VALUES(?,?,?,?,?,?)',samples)
    with journal.db() as db:db.execute('PRAGMA journal_mode=DELETE')
    (folder/'testbench/response_receipts.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in receipts),encoding='utf-8')
    put(folder/'testbench/collector_closed.json',dict(run=run,own_child_exited=True,collector_exited=True,scheduled=total,submitted=total,skipped=0))
    put(folder/'collection_status.json',dict(run=run,status='collection_complete_awaiting_measurement_audit',input_profile=PROFILE))
    code_snapshot(folder);seal_collection(folder,run)
    return folder


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary=tempfile.TemporaryDirectory();cls.root=Path(cls.temporary.name)
        cls.collection=fixture(cls.root/'collection')
        cls.processing=cls.root/'positive'
        cls.started=time.perf_counter();cls.result=pipeline.process(cls.collection,cls.processing)
        cls.seconds=time.perf_counter()-cls.started

    @classmethod
    def tearDownClass(cls):cls.temporary.cleanup()

    def test_end_to_end_export_gate_four_outputs_quality(self):
        self.assertEqual(self.result['status'],'processing_complete')
        report=pipeline.read(self.processing/'measurement_audit.json')
        self.assertTrue(report['admitted']);self.assertEqual(report['ordinary_width_pass_fraction'],1)
        quality=pipeline.read(self.processing/'quality.json')
        self.assertEqual(set(quality['methods']),set(pipeline.NAMES));self.assertEqual(len(quality['events']),2)
        self.assertEqual(quality['post_warmup_rows'],390)
        self.assertTrue((self.processing/'scoring_complete.json').exists())
        exported=pipeline.lines(self.processing/'measurements.jsonl')
        self.assertNotIn('served_mode',exported[1]);self.assertNotIn('iterations',exported[1])
        self.assertFalse(pipeline.read(self.processing/'export_complete.json')['outcomes_parsed'])

    def test_export_does_not_read_outcomes(self):
        with patch.object(pipeline,'load_outcomes',side_effect=AssertionError('No outcomes during export')):
            pipeline.export(self.collection,self.root/'export-only')

    def test_cannot_overwrite_existing_processing(self):
        with self.assertRaises(FileExistsError):pipeline.export(self.collection,self.processing)
        with self.assertRaises(FileExistsError):pipeline.quality(self.processing)

    def test_quality_does_not_open_outcomes_without_four_outputs_barrier(self):
        output=self.root/'no-barrier';pipeline.export(self.collection,output);pipeline.audit(output)
        with patch.object(pipeline,'load_outcomes',side_effect=AssertionError('Quality read before barrier')) as load:
            with self.assertRaises(FileNotFoundError):pipeline.quality(output)
            load.assert_not_called()

    def test_scoring_cannot_start_before_measurement_audit(self):
        output=self.root/'no-audit';pipeline.export(self.collection,output)
        with patch.object(pipeline,'save_outputs') as save:
            with self.assertRaises(FileNotFoundError):pipeline.score(output)
            save.assert_not_called()

    def test_modified_scored_file_is_rejected_before_quality_reads(self):
        output=self.root/'tampered-score';shutil.copytree(self.processing,output)
        (output/'four_outputs/ipca.jsonl').write_text('{}\n',encoding='utf-8')
        with patch.object(pipeline,'load_outcomes') as load:
            with self.assertRaises(ValueError):pipeline.quality(output)
            load.assert_not_called()

    def test_modified_export_is_rejected_before_any_quality_read(self):
        output=self.root/'tampered-export';shutil.copytree(self.processing,output)
        with (output/'measurements.jsonl').open('a',encoding='utf-8') as stream:stream.write('{}\n')
        with patch.object(pipeline,'load_outcomes') as load:
            with self.assertRaises(ValueError):pipeline.quality(output)
            load.assert_not_called()

    def test_modified_scoring_binding_rejected(self):
        output=self.root/'tampered-binding';shutil.copytree(self.processing,output)
        binding=pipeline.read(output/'scoring_complete.json');binding['measurement_audit_sha256']='0'*64
        put(output/'scoring_complete.json',binding)
        with patch.object(pipeline,'load_outcomes') as load:
            with self.assertRaises(ValueError):pipeline.quality(output)
            load.assert_not_called()

    def test_failed_measurement_gate_stops_before_scoring(self):
        collection=fixture(self.root/'wide-collection',wide=True);output=self.root/'wide-output'
        with patch.object(pipeline,'save_outputs') as save:
            result=pipeline.process(collection,output);save.assert_not_called()
        self.assertFalse(result['quality_evaluated']);self.assertEqual(result['status'],'stopped_at_measurement_gate')
        self.assertFalse(pipeline.read(output/'measurement_audit.json')['gates']['read_alignment'])
        self.assertFalse((output/'four_outputs').exists());self.assertFalse((output/'quality.json').exists())

    def test_changed_closed_database_fails_collection_seal(self):
        collection=self.root/'changed-db';shutil.copytree(self.collection,collection)
        with closing(sqlite3.connect(collection/'application_snapshot.sqlite3')) as db:
            db.execute('UPDATE requests SET status=503 WHERE id=1');db.commit()
        with self.assertRaises(ValueError):pipeline.export(collection,self.root/'changed-db-output')

    def test_unsealed_sqlite_sidecar_is_rejected(self):
        collection=self.root/'sidecar-db';shutil.copytree(self.collection,collection)
        Path(str(collection/'application_snapshot.sqlite3')+'-wal').write_bytes(b'unsealed')
        with self.assertRaises(ValueError):pipeline.export(collection,self.root/'sidecar-output')

    def reseal_fixture(self,collection):
        seal=pipeline.read(collection/'collection_complete.json')
        for record in seal['files']:
            record['sha256']=sha(collection/record['path']);record['bytes']=(collection/record['path']).stat().st_size
        put(collection/'collection_complete.json',seal)

    def test_bad_raw_counter_stops_export_without_outcome_reads(self):
        collection=self.root/'counter-reset';shutil.copytree(self.collection,collection)
        with closing(sqlite3.connect(collection/'application_snapshot.sqlite3')) as db:
            raw=json.loads(db.execute('SELECT counters FROM samples WHERE id=20').fetchone()[0]);raw['requests_completed']=0
            db.execute('UPDATE samples SET counters=? WHERE id=20',(json.dumps(raw),));db.commit()
        self.reseal_fixture(collection)
        with patch.object(pipeline,'load_outcomes') as load:
            with self.assertRaises(ValueError):pipeline.export(collection,self.root/'counter-reset-output')
            load.assert_not_called()

    def test_failed_request_stops_at_measurement_gate(self):
        collection=self.root/'request-error';shutil.copytree(self.collection,collection)
        with closing(sqlite3.connect(collection/'application_snapshot.sqlite3')) as db:
            db.execute('UPDATE requests SET status=503 WHERE id=1');db.commit()
        receipts=pipeline.lines(collection/'testbench/response_receipts.jsonl');receipts[0]['status']=503
        (collection/'testbench/response_receipts.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in receipts),encoding='utf-8')
        self.reseal_fixture(collection)
        with patch.object(pipeline,'save_outputs') as save:
            result=pipeline.process(collection,self.root/'request-error-output');save.assert_not_called()
        self.assertFalse(result['quality_evaluated'])

    def test_missing_initial_load_group_stops_measurement_check(self):
        collection=self.root/'missing-group';shutil.copytree(self.collection,collection)
        log=pipeline.lines(collection/'schedule_log.jsonl');log=[r for r in log if r['samples']!=32]
        (collection/'schedule_log.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in log),encoding='utf-8')
        self.reseal_fixture(collection)
        with patch.object(pipeline,'load_outcomes') as load:
            with self.assertRaises(ValueError):pipeline.process(collection,self.root/'missing-group-output')
            load.assert_not_called()

    def test_backup_includes_committed_wal_and_closes_snapshot(self):
        from . import cycle_collection
        collection=self.root/'wal-backup';connection=None
        def fake_base(output,order,profile,bench_factory):
            nonlocal connection
            self.assertIs(bench_factory,RecordedCycleBench);fixture(collection)
            (collection/'collection_complete.json').unlink() # Only this test's new temporary fixture.
            source=collection/'testbench/application.sqlite3'
            shutil.copyfile(collection/'application_snapshot.sqlite3',source)
            connection=sqlite3.connect(source);connection.execute('PRAGMA journal_mode=WAL')
            connection.execute('CREATE TABLE committed_wal_probe(value INTEGER)')
            connection.execute('INSERT INTO committed_wal_probe VALUES(123)');connection.commit()
            self.assertTrue(Path(str(source)+'-wal').exists())
            return {'run':'fixture-only'}
        try:
            with patch.object(cycle_collection,'base_collect',side_effect=fake_base):cycle_collection.collect(collection,1)
            pipeline.collection_info(collection)
            with pipeline.readonly(collection/'application_snapshot.sqlite3') as db:
                self.assertEqual(db.execute('SELECT value FROM committed_wal_probe').fetchone()[0],123)
        finally:
            if connection:connection.close()

    def test_cycle_collection_preview_never_starts_workload(self):
        from .cycle_collection import main
        from contextlib import redirect_stdout
        from io import StringIO
        with patch('sys.argv',['cycle_collection','--order','2']),patch('monitor_service.cycle_collection.collect') as collect,redirect_stdout(StringIO()) as stdout:
            main();collect.assert_not_called()
        self.assertFalse(json.loads(stdout.getvalue())['collection_started'])


class EventTests(unittest.TestCase):
    def record(self,t,alarm,available=None):return dict(timestamp=t,alarm=alarm,available_at=t if available is None else available)

    def test_preexisting_alarm_not_counted_as_event_detection(self):
        records=[self.record(t,True) for t in range(25)]
        result=pipeline.summarize_method(records,[dict(start=10,end=20)],[])
        self.assertFalse(result['events'][0]['detected_before_end']);self.assertEqual(len(result['notifications']),1)

    def test_availability_after_event_end_is_late(self):
        result=pipeline.summarize_method([self.record(11,True,15.1)],[dict(start=10,end=15)],[])
        self.assertFalse(result['events'][0]['detected_before_end']);self.assertEqual(result['events'][0]['late_availability_count'],1)
        self.assertIsNone(result['events'][0]['delay_seconds'])

    def test_ten_observed_quiet_seconds_start_new_episode(self):
        records=[self.record(0,True)]+[self.record(t,False) for t in range(1,12)]+[self.record(12,True)]
        found,_=pipeline.episodes(records);self.assertEqual([f['start'] for f in found],[0,12])

    def test_missing_or_unknown_is_not_recovery(self):
        for records in ([self.record(0,True),self.record(1,False),self.record(20,False),self.record(21,True)],
                        [self.record(0,True),self.record(1,False),self.record(2,None),self.record(12,True)]):
            self.assertEqual(len(pipeline.episodes(records)[0]),1)

    def test_alarm_duration_has_no_extrapolated_tail_or_gap(self):
        records=[self.record(0,True),self.record(1,True),self.record(10,False)]
        self.assertEqual(pipeline.episodes(records)[1],1)


class ReceiptTransportTest(unittest.TestCase):
    def test_short_real_receipt_saved_and_child_stopped(self):
        from .store import Store
        with tempfile.TemporaryDirectory() as directory:
            bench=RecordedCycleBench(Store(Path(directory)/'monitor.sqlite3'))
            try:
                bench.start();deadline=time.monotonic()+8
                while time.monotonic()<deadline:
                    path=bench.root/'response_receipts.jsonl'
                    if path.exists() and len(pipeline.lines(path))>=2:break
                    time.sleep(.05)
                records=pipeline.lines(path);self.assertGreaterEqual(len(records),2)
                self.assertEqual(len({r['body_sha256'] for r in records}),1)
                self.assertTrue(all(r['status']==200 and not r['error'] for r in records))
            finally:bench.close()
            closed=pipeline.read(bench.root/'collector_closed.json')
            self.assertTrue(closed['collector_exited'] and closed['own_child_exited'])
            with bench.journal.db() as db:count=db.execute('SELECT count(*) FROM requests').fetchone()[0]
            self.assertEqual(count,closed['submitted']);self.assertEqual(count,len(pipeline.lines(path)))


if __name__=='__main__':unittest.main(verbosity=2)
