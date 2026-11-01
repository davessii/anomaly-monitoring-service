"""Software semantics only, with disposable deterministic numeric fixtures."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from .detector import Detector, cut, np
from .immediate import ImmediateDetector, MODE, restore_detector
from .store import Store, encode
from .test_service import samples, observations

ROOT=Path(__file__).resolve().parents[1]

class ImmediateTests(unittest.TestCase):
    def ready(self):
        raw=samples(960,4)
        d=ImmediateDetector([f'f{i}' for i in range(4)],components=2)
        for i,row in enumerate(raw[:896]):self.assertEqual(d.observe(i+1,row),[])
        return d,raw

    def test_first_row_has_decision_and_model_changes_only_after_16(self):
        d,raw=self.ready()
        initial=d.to_state();model=encode(initial['model'])
        for i in range(896,911):
            output=d.observe(i+1,raw[i])
            self.assertEqual(len(output),1)
            self.assertEqual(output[0]['decision_source_time'],i+1)
            self.assertEqual(encode(d.to_state()['model']),model)
            self.assertEqual(encode(d.records),encode(initial['records']))
            self.assertEqual(d.history.tobytes(),np.array(initial['history']).tobytes())
        output=d.observe(912,raw[911])
        self.assertEqual(len(output),1)
        self.assertNotEqual(encode(d.to_state()['model']),model)
        self.assertEqual(d.pending,[])
        self.assertEqual(d.live_block,{'base':False,'peak':0.})
        self.assertEqual(d.records[-1]['base'],bool(any(x['base_alarm'] for x in self._block_decisions(raw))))

    def _block_decisions(self,raw):
        d=ImmediateDetector([f'f{i}' for i in range(4)],components=2)
        out=[]
        for i,r in enumerate(raw[:912]):out.extend(d.observe(i+1,r))
        return out

    def test_scores_use_prior_model_and_same_row_delta(self):
        d,raw=self.ready()
        for i in range(896,928):
            control=Detector.from_state(json.loads(encode(d.to_state())))
            if d.pending:
                control.previous=(np.array(d.pending[-1][1])-control.center)/control.scale
            threshold=cut(control.records)
            # Independent legacy score formula; skip learning in this disposable
            # copy because a rank-2 model cannot be trained on a single row.
            with patch.object(control.model,'update'):
                v=control.trace([raw[i]])
            base,aux=control.route(v)
            result=d.observe(i+1,raw[i])[0]
            self.assertEqual(result['alarm'],bool(base[0] or aux[0]>threshold))
            self.assertEqual(result['auxiliary_score'],float(aux[0]))
            self.assertEqual(result['q_score'],float(v['scores'][0,0]/control.channel_scales[0]))
            self.assertEqual(result['auxiliary_threshold'],None if np.isinf(threshold) else threshold)

    def test_model_and_reference_update_match_legacy_blocks(self):
        d,raw=self.ready()
        legacy=Detector([f'f{i}' for i in range(4)],components=2)
        for i,r in enumerate(raw[:896]):legacy.observe(i+1,r)
        for i in range(896,len(raw)):
            d.observe(i+1,raw[i]);legacy.observe(i+1,raw[i])
            if (i+1-896)%16==0:
                for key in ('model','history','rates','previous'):
                    self.assertEqual(encode(d.to_state()[key]),encode(legacy.to_state()[key]))

    def test_restart_and_future_changes_cannot_change_prefix_or_current_cut(self):
        d,raw=self.ready()
        for i in range(896,903):d.observe(i+1,raw[i])
        restored=restore_detector(json.loads(encode(d.to_state())))
        self.assertIsInstance(restored,ImmediateDetector)
        for i in range(903,928):
            self.assertEqual(encode(d.observe(i+1,raw[i])),encode(restored.observe(i+1,raw[i])))
        self.assertEqual(encode(d.to_state()),encode(restored.to_state()))
        left,raw=self.ready();right=restore_detector(json.loads(encode(left.to_state())))
        for i in range(896,920):
            a=left.observe(i+1,raw[i]);b=right.observe(i+1,raw[i] if i<905 else raw[i]+100)
            if i<905:self.assertEqual(encode(a),encode(b))
            # Even future rows in this block cannot influence its earlier cut.
            if i<912:self.assertEqual(a[0]['auxiliary_threshold'],b[0]['auxiliary_threshold'])

    def test_store_immediate_decisions_persist_and_failed_ingest_rolls_back(self):
        parent=ROOT/'work/service-test-tmp';parent.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=parent) as temporary:
            store=Store(Path(temporary)/'db.sqlite3')
            store.register({'id':'new','features':[f'f{i}' for i in range(4)],'settings':{'components':2,'processing_mode':MODE}})
            raw=samples(928,4);rows=observations(raw)
            for a in range(0,896,128):store.ingest('new',{'observations':rows[a:a+128]})
            out=store.ingest('new',{'observations':rows[896:903]})
            self.assertEqual(len(out['decisions']),7)
            before=store.history('new',512);before_state=store.state('new')
            original=ImmediateDetector.observe
            def fail_second(obj,ts,values):
                if ts==905:raise ValueError('technical fixture rollback')
                return original(obj,ts,values)
            with patch.object(ImmediateDetector,'observe',fail_second):
                with self.assertRaises(ValueError):store.ingest('new',{'observations':rows[903:905]})
            self.assertEqual(before,store.history('new',512));self.assertEqual(before_state,store.state('new'))
            reopened=Store(store.path)
            out=reopened.ingest('new',{'observations':rows[903:]})
            self.assertEqual(len(out['decisions']),25)
            self.assertEqual(reopened.state('new')[0]['pending'],0)
            self.assertEqual(reopened.state('new')[0]['notification_unit'],'observation')
            self.assertEqual(sum(r['decision'] is not None for r in reopened.history('new',512)['observations']),32)
            self.assertTrue(all(r['timestamp']==r['decision']['decision_source_time'] for r in reopened.history('new',512)['observations'] if r['decision']))

    def test_legacy_states_stay_legacy_and_unknown_modes_are_rejected(self):
        old=Detector(['a','b'],components=1)
        self.assertIs(type(restore_detector(old.to_state())),Detector)
        self.assertNotIn('processing_mode',old.to_state())
        with self.assertRaises(ValueError):restore_detector(dict(old.to_state(),processing_mode='unknown'))

if __name__=='__main__':unittest.main(verbosity=2)
