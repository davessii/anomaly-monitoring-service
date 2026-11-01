"""Disposable end-to-end persistence fixtures; virtual timestamps are explicit."""
import json
from pathlib import Path
import tempfile
import unittest

from .detector import np
from .immediate import restore_detector
from .log_detector import LogDetector,MODE
from .log_precise import PROFILE,PreciseLogAggregator,parse_precise_line,precise_line
from .log_checkpoint import pack,unpack,window_from_dict
from .log_ingest import LogIngestor,LogIngestError,digest,canonical
from .log_schema import FEATURES
from .log_tailer import LogTailer
from .store import Store
from .test_log_candidate_v2 import fixture
from .test_log_tailer import BASE


def prepared():
    detector=LogDetector(FEATURES,handler_limit_ms=10)
    for i,row in enumerate(fixture(896,471)):detector.observe(BASE/1000+i+1,row.tolist())
    return detector


class PersistedLogDetectorTests(unittest.TestCase):
    def test_warmup_and_microsecond_values_survive_partial_restart(self):
        d=LogDetector(FEATURES,handler_limit_ms=10)
        rows=fixture(930,471)
        for i,row in enumerate(rows[:500]):last=d.observe(i+1,row.tolist())[0]
        self.assertEqual(last['message_kind'],'training')
        saved=json.loads(json.dumps(d.to_state()))
        other=restore_detector(saved)
        for i,row in enumerate(rows[500:],501):self.assertEqual(d.observe(i,row.tolist()),other.observe(i,row.tolist()))
        self.assertEqual(d.to_state(),other.to_state())
        self.assertEqual(other.phase,'monitoring')

    def test_deferred_block_survives_restart_and_recovery_requires_ten_good_windows(self):
        d=prepared();start=BASE/1000+897
        self.assertEqual(d.observe(start,[2.,50.,0.])[0]['message_kind'],'alarm')
        other=restore_detector(json.loads(json.dumps(d.to_state())))
        kinds=[]
        for i,row in enumerate(fixture(32,829),1):
            a=d.observe(start+i,row.tolist());b=other.observe(start+i,row.tolist())
            self.assertEqual(a,b);kinds.append(a[0]['message_kind'])
        self.assertEqual(kinds[:9],['recovering']*9)
        self.assertEqual(kinds[9],'recovery')
        self.assertEqual(kinds.count('recovery'),1)
        self.assertEqual(d.candidate.update_counts['deferred'],1)
        self.assertEqual(d.to_state(),other.to_state())

    def test_gap_does_not_confirm_recovery(self):
        d=prepared();start=BASE/1000+897
        d.observe(start,[2.,50.,0.])
        for i in range(1,7):d.observe(start+i,[2.,.5,0.])
        row=d.observe(start+100,[2.,.5,0.])[0]
        self.assertEqual(row['message_kind'],'recovering')
        self.assertEqual(row['quality_state']['good_count'],1)

    def test_invalid_calibration_is_not_presented_as_ready(self):
        d=LogDetector(FEATURES,handler_limit_ms=10)
        for i in range(896):row=d.observe(i+1,[2.,0.,0.])[0]
        self.assertEqual(row['message_kind'],'unfit')
        self.assertFalse(d.ready)
        self.assertEqual(restore_detector(json.loads(json.dumps(d.to_state()))).phase,'unfit')

    def test_bad_quality_during_warmup_is_latched_without_learning_it(self):
        d=LogDetector(FEATURES,handler_limit_ms=10)
        self.assertEqual(d.observe(1,[2.,50.,0.])[0]['message_kind'],'alarm')
        for i,row in enumerate(fixture(900,829),2):last=d.observe(i,row.tolist())[0]
        self.assertEqual(last['message_kind'],'unfit')
        self.assertEqual(d.core.seen,0)
        self.assertEqual(d.seen,901)
        self.assertFalse(d.ready)
        self.assertEqual(restore_detector(json.loads(json.dumps(d.to_state()))).invalid_reason,'quality_violation_during_warmup')

    def test_model_anomaly_with_good_quality_is_warning_not_proven_fault(self):
        d=prepared();row=d.observe(BASE/1000+897,[9.,.5,0.])[0]
        self.assertTrue(row['statistical_alarm'])
        self.assertFalse(row['quality_alarm'])
        self.assertEqual(row['message_kind'],'warning')


class PreciseIngestTests(unittest.TestCase):
    def setUp(self):
        self.root=Path(__file__).resolve().parents[1]/'work/log-integrated-v2-test-tmp'
        self.root.mkdir(exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(dir=self.root)
        self.addCleanup(self.cleanup)
        self.folder=Path(self.temp.name);self.path=self.folder/'app.log';self.path.write_bytes(b'')
        self.dbpath=self.folder/'monitor.sqlite3';self.store=Store(self.dbpath)
        self.store.register(dict(id='log',features=list(FEATURES),settings=dict(processing_mode=MODE,handler_limit_ms=10)))
        self.ingest=LogIngestor(self.store)
        self.ingest.attach('source','log',self.path,start_ms=BASE,profile=PROFILE,settings=dict(handler_limit_ms=10))

    def cleanup(self):
        target=Path(self.temp.name).resolve()
        if not target.is_relative_to(self.root.resolve()) or target==self.root.resolve():raise RuntimeError('Unsafe cleanup')
        self.temp.cleanup()

    def append(self,value):
        with self.path.open('ab',buffering=0) as f:f.write(value)

    def counts(self):
        with self.store.connection() as db:
            return {name:db.execute(f'SELECT count(*) FROM {name}').fetchone()[0] for name in ('log_records','log_messages','observations','notifications','log_windows')}

    def test_exact_units_checkpoint_and_atomic_idempotent_delivery(self):
        self.append(precise_line(BASE+100,200,250_000)+precise_line(BASE+500,200,750_000))
        self.ingest.poll('source',now_ms=BASE+800)
        with self.store.connection() as db:
            cp=json.loads(db.execute('SELECT state FROM log_checkpoints').fetchone()[0])
            self.assertEqual(cp['version'],2)
            self.assertEqual([r[0] for r in db.execute('SELECT request_time_us FROM log_records ORDER BY start_offset')],[250,750])
        self.store=Store(self.dbpath);self.ingest=LogIngestor(self.store)
        self.ingest.poll('source',now_ms=BASE+3000)
        saved=self.ingest.windows('source')[0]
        payload=window_from_dict(json.loads(self._payload())).as_dict()
        self.assertEqual(payload['duration_sum_us'],1000)
        self.assertEqual(self.ingest.status('source')['reader']['profile'],PROFILE)
        result=self.ingest.deliver('source',now_ms=BASE+3000)
        self.assertEqual(result['status'],'delivered')
        before=self.counts()
        self.assertEqual(self.ingest.deliver('source',now_ms=BASE+3000,start_ms=BASE,expected_sha256=saved['sha256'])['status'],'already_delivered')
        self.assertEqual(before,self.counts())
        row=self.store.history('log')['observations'][-1]
        self.assertEqual(row['values']['mean_request_ms'],.5)
        self.assertEqual(row['decision']['message_kind'],'training')

    def _payload(self):
        with self.store.connection() as db:return db.execute('SELECT payload FROM log_windows ORDER BY start_ms LIMIT 1').fetchone()[0]

    def test_failed_future_batch_does_not_commit_partial_raw_records(self):
        self.append(precise_line(BASE+100,200,250_000)+precise_line(BASE+900,200,750_000))
        with self.assertRaises(LogIngestError):self.ingest.poll('source',now_ms=BASE+800)
        self.assertEqual(self.counts()['log_records'],0)
        self.assertEqual(self.ingest.status('source')['reader']['read_offset'],0)

    def test_fault_after_ingest_rolls_back_message_model_and_receipt(self):
        self.append(precise_line(BASE+100,200,250_000))
        self.ingest.poll('source',now_ms=BASE+800);self.ingest.poll('source',now_ms=BASE+3000)
        before=self.counts()
        def fail(stage):
            if stage=='after_ingest':raise RuntimeError('fixture rollback')
        self.ingest._fault_hook=fail
        with self.assertRaises(RuntimeError):self.ingest.deliver('source',now_ms=BASE+3000)
        self.assertEqual(before,self.counts())
        self.assertEqual(self.store.state('log')[0]['samples'],0)

    def test_full_virtual_stream_parser_learning_alarm_recovery_and_restart(self):
        raw=np.vstack((fixture(896,471),fixture(4,829),[2.,50.,0.],fixture(20,829)))
        for i,row in enumerate(raw):
            count=int(row[0]);us=int(round(row[1]*1000))
            for n in range(count):self.append(precise_line(BASE+i*1000+100+n*100,200,us*1000))
            now=BASE+i*1000+800
            self.ingest.poll('source',now_ms=now)
            while self.ingest.deliver('source',now_ms=now)['status']!='nothing_ready':pass
            if i==905:
                self.store=Store(self.dbpath);self.ingest=LogIngestor(self.store)
        now=BASE+(len(raw)-1)*1000+3000
        self.ingest.poll('source',now_ms=now)
        while self.ingest.deliver('source',now_ms=now)['status']!='nothing_ready':pass
        self.assertEqual(self.store.state('log')[0]['samples'],len(raw))
        self.assertEqual(self.store.state('log')[0]['phase'],'monitoring')
        with self.store.connection() as db:
            kinds=[r[0] for r in db.execute('SELECT kind FROM log_messages ORDER BY ts')]
            data=json.loads(db.execute('SELECT state FROM hosts WHERE id=?',('log',)).fetchone()[0])
        self.assertIn('training',kinds);self.assertIn('alarm',kinds);self.assertIn('recovery',kinds)
        self.assertGreaterEqual(data['candidate']['update_counts']['deferred'],1)
        self.assertEqual(data['log_settings']['handler_limit_ms'],10.)


class PreciseWorkerSettingsTests(unittest.TestCase):
    def setUp(self):
        from .log_worker import LogWorkers
        root=Path(__file__).resolve().parents[1]/'work/log-integrated-v2-test-tmp'
        root.mkdir(exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(dir=root);self.addCleanup(self.temp.cleanup)
        self.folder=Path(self.temp.name);self.store=Store(self.folder/'monitor.sqlite3')
        self.workers=LogWorkers(self.store,self.folder/'logs',clock_ms=lambda:BASE)
        self.addCleanup(self.workers.close)
        (self.workers.directory/'app.log').write_bytes(b'')

    def test_bad_settings_leave_no_unused_model(self):
        for value in (None,0,True,float('nan')):
            with self.assertRaises(ValueError):self.workers.start(dict(filename='app.log',profile=PROFILE,handler_limit_ms=value))
        self.assertEqual(self.store.state(),[])

    def test_resume_keeps_profile_and_settings_and_cannot_reconfigure(self):
        from .log_worker import LogWorkers
        created=self.workers.start(dict(filename='app.log',profile=PROFILE,handler_limit_ms=7.5,allowed_error_fraction=.02))
        self.workers.close()
        state=self.store.state(created['host'])[0]
        self.assertEqual(state['processing_mode'],MODE)
        self.assertEqual(state['log_settings']['handler_limit_ms'],7.5)
        workers=LogWorkers(Store(self.store.path),self.folder/'logs',clock_ms=lambda:BASE)
        self.addCleanup(workers.close)
        with self.assertRaises(ValueError):workers.start(dict(source_id=created['id'],handler_limit_ms=50))
        result=workers.start(dict(source_id=created['id']))
        self.assertEqual(result['host'],created['host'])
        source=workers.sources()[0]
        self.assertEqual(source['config']['profile'],PROFILE)
        self.assertEqual(source['config']['log_settings'],state['log_settings'])
        self.assertEqual(source['reader']['profile'],PROFILE)


if __name__=='__main__':unittest.main(verbosity=2)
