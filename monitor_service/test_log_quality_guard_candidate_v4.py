"""Explicit quality requirement, causal gate and unchanged raw alarms."""
import copy
import unittest
from unittest.mock import patch

from .detector import np
from .log_guard_candidate_v3 import GuardedLogCandidate
from .log_quality_guard_candidate_v4 import QualityGuardCandidate
from .test_log_candidate_v2 import fixture,metrics
from .test_log_guard_candidate_v3 import state


def ready(*,recovery=False):
    candidate=QualityGuardCandidate(handler_limit_ms=10,mark_recovery=recovery)
    for i,row in enumerate(fixture(896,471)):candidate.observe(i+1,metrics(row))
    return candidate


def baseline():
    candidate=GuardedLogCandidate()
    for i,row in enumerate(fixture(896,471)):candidate.observe(i+1,metrics(row))
    return candidate


class QualityGuardTests(unittest.TestCase):
    def test_handler_requirement_is_mandatory_validated_and_separate(self):
        with self.assertRaises(TypeError):QualityGuardCandidate()
        for value in (None,True,0,-1,float('inf'),float('nan'),'10',1e13):
            with self.subTest(value=value),self.assertRaises(ValueError):
                QualityGuardCandidate(handler_limit_ms=value)
        self.assertEqual(QualityGuardCandidate(handler_limit_ms=10).handler_limit_ms,10.)

    def test_normal_outputs_and_model_equal_unguarded_candidate(self):
        a=baseline();b=ready()
        for i,row in enumerate(fixture(128,829)):
            left=a.observe(897+i,metrics(row));right=b.observe(897+i,metrics(row))
            self.assertEqual(left['ipca_decision'],right['ipca_decision'])
            self.assertEqual(left['alarm'],right['alarm'])
            self.assertFalse(right['quality_bad'])
        self.assertEqual(state(a),state(b))

    def test_growth_in_requests_does_not_forbid_learning(self):
        a=baseline();b=ready()
        raw=fixture(32,915);raw[:,0]+=6
        primary=0
        for i,row in enumerate(raw):
            left=a.observe(897+i,metrics(row));right=b.observe(897+i,metrics(row))
            self.assertEqual(left['ipca_decision'],right['ipca_decision'])
            primary+=right['ipca_decision']['base_alarm']
            self.assertFalse(right['quality_bad'])
        self.assertGreater(primary,0)  # Tests admission despite a real PCA flag.
        self.assertEqual(b.update_counts,dict(attempted=2,accepted=2,deferred=0))
        self.assertEqual(state(a),state(b))

    def test_exactly_at_limit_is_accepted_even_if_ipca_dislikes_it(self):
        candidate=ready()
        for i,row in enumerate(fixture(15,829)):candidate.observe(897+i,metrics(row))
        result=candidate.observe(912,metrics([2.,10.,0.]))
        self.assertFalse(result['quality_bad'])
        self.assertTrue(result['learning']['accepted'])
        self.assertEqual(candidate.update_counts['deferred'],0)

    def test_above_limit_defers_bundle_without_backdating_current_decision(self):
        a=baseline();b=ready()
        for i,row in enumerate(fixture(15,829)):
            a.observe(897+i,metrics(row));b.observe(897+i,metrics(row))
        before=state(b)
        first=a.observe(912,metrics([2.,10.001,0.]))
        second=b.observe(912,metrics([2.,10.001,0.]))
        self.assertEqual(first['ipca_decision'],second['ipca_decision'])
        self.assertTrue(second['quality_bad'])
        self.assertTrue(second['learning']['deferred'])
        for key in ('model','history','rates'):self.assertEqual(before[key],state(b)[key])
        expected=(np.array([2.,10.001,0.])-b.detector.center)/b.detector.scale
        self.assertEqual(state(b)['previous'],expected.tolist())

    def test_explicit_error_can_block_learning_without_ipca_alarm(self):
        candidate=ready()
        event=candidate.observe(897,metrics([2.,.5,.5]))
        self.assertFalse(event['ipca_decision']['alarm'])
        self.assertTrue(event['quality_bad'])
        self.assertEqual(event['quality_reasons'],['http_errors_above_limit'])
        for i,row in enumerate(fixture(15,829)):result=candidate.observe(898+i,metrics(row))
        self.assertTrue(result['learning']['deferred'])

    def test_latency_requirement_is_gate_only_not_an_added_alarm(self):
        candidate=ready()
        with patch.object(candidate.detector,'route',return_value=(np.array([False]),np.array([0.]))):
            result=candidate.observe(897,metrics([2.,50.,0.]))
        self.assertTrue(result['quality_bad'])
        self.assertFalse(result['ipca_decision']['alarm'])
        self.assertFalse(result['alarm'])
        self.assertEqual(result['signal_sources'],[])

    def test_recovery_label_cannot_change_quality_gate_or_raw_state(self):
        a=ready();b=ready(recovery=True)
        rows=np.vstack(([2.,50.,0.],fixture(64,829)))
        for i,row in enumerate(rows):
            left=a.observe(897+i,metrics(row));right=b.observe(897+i,metrics(row))
            for key in ('ipca_decision','alarm','quality_bad','quality_reasons','learning'):
                self.assertEqual(left[key],right[key])
            self.assertEqual(state(a),state(b))
        self.assertEqual(a.update_counts,b.update_counts)

    def test_rejected_block_keeps_actual_last_sample_for_next_difference(self):
        candidate=ready()
        candidate.observe(897,metrics([2.,50.,0.]))
        rows=fixture(15,829)
        for i,row in enumerate(rows):candidate.observe(898+i,metrics(row))
        self.assertEqual(candidate.update_counts['deferred'],1)
        original=candidate.detector.route
        with patch.object(candidate.detector,'route',wraps=original) as routed:
            candidate.observe(913,metrics(rows[-1]))
        self.assertEqual(float(routed.call_args.args[0]['er'][0,0]),0.)

    def test_invalid_row_cannot_poison_quality_block(self):
        candidate=ready()
        candidate.observe(897,metrics([2.,50.,0.]))
        before=state(candidate)
        flags=(candidate.block_quality_bad,copy.deepcopy(candidate.update_counts),
               candidate.last_timestamp,copy.deepcopy(candidate.previous_result))
        for timestamp,value in ((897,metrics([2.,.5,0.])),(898,metrics([2.,-1.,0.]))):
            with self.assertRaises(ValueError):candidate.observe(timestamp,value)
            self.assertEqual(state(candidate),before)
            self.assertEqual(flags,(candidate.block_quality_bad,candidate.update_counts,
                                    candidate.last_timestamp,candidate.previous_result))


if __name__=='__main__':unittest.main(verbosity=2)
