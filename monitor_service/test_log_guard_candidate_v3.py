"""Causality, factor separation and update-state semantics; no quality claims."""
import copy
import json
import unittest
from unittest.mock import patch

from .detector import np
from .log_candidate_v2 import CheckedLogCandidate
from .log_guard_candidate_v3 import GuardedLogCandidate
from .test_log_candidate_v2 import fixture,metrics


def ready(*,protect=False,recovery=False,rounded=False):
    candidate=GuardedLogCandidate(protect_learning=protect,mark_recovery=recovery)
    raw=fixture(896,471)
    if rounded:raw[:,1]=0
    for i,row in enumerate(raw):candidate.observe(i+1,metrics(row))
    return candidate


def state(candidate):
    return json.loads(json.dumps(candidate.detector.to_state()))


class GuardCandidateTests(unittest.TestCase):
    def test_disabled_factors_preserve_v2_outputs_and_state(self):
        original=CheckedLogCandidate()
        candidate=GuardedLogCandidate()
        rows=np.vstack((fixture(896,471),[2.,50.,0.],fixture(128,829)))
        for i,row in enumerate(rows):
            first=original.observe(i+1,metrics(row))
            second=candidate.observe(i+1,metrics(row))
            for key,value in first.items():self.assertEqual(second[key],value)
        self.assertEqual(state(original),state(candidate))

    def test_recovery_factor_does_not_change_raw_decisions_or_learning(self):
        for protect in (False,True):
            with self.subTest(protect=protect):
                a=ready(protect=protect)
                b=ready(protect=protect,recovery=True)
                rows=np.vstack(([2.,50.,0.],fixture(128,829)))
                for i,row in enumerate(rows):
                    left=a.observe(897+i,metrics(row));right=b.observe(897+i,metrics(row))
                    for key in ('alarm','raw_alarm','ipca_decision','http_error_alarm','learning','signal_sources'):
                        self.assertEqual(left[key],right[key])
                    self.assertEqual(state(a),state(b))

    def test_suspect_block_defers_model_history_and_rates_but_keeps_last_row(self):
        candidate=ready(protect=True)
        before=state(candidate)
        values=np.vstack(([2.,50.,0.],fixture(15,829)))
        for i,row in enumerate(values):result=candidate.observe(897+i,metrics(row))
        self.assertTrue(result['learning']['deferred'])
        after=state(candidate)
        for key in ('model','history','rates'):self.assertEqual(before[key],after[key])
        self.assertNotEqual(before['records'],after['records'])
        self.assertEqual(after['pending'],[])
        expected=(values[-1]-candidate.detector.center)/candidate.detector.scale
        self.assertEqual(after['previous'],expected.tolist())
        self.assertEqual(candidate.update_counts,dict(attempted=1,accepted=0,deferred=1))
        # Equal actual consecutive observations have zero delta; a rejected
        # learning block must not create a guessed gap in the time sequence.
        original_route=candidate.detector.route
        with patch.object(candidate.detector,'route',wraps=original_route) as routed:
            following=candidate.observe(913,metrics(values[-1]))
        self.assertEqual(float(routed.call_args.args[0]['er'][0,0]),0.)
        self.assertEqual(following['ipca_decision']['timestamp'],913)

    def test_current_last_row_cannot_change_its_own_score_or_escape_gate(self):
        baseline=ready()
        candidate=ready(protect=True)
        for i,row in enumerate(fixture(15,829)):
            self.assertEqual(baseline.observe(897+i,metrics(row))['ipca_decision'],
                             candidate.observe(897+i,metrics(row))['ipca_decision'])
        old=state(candidate)
        first=baseline.observe(912,metrics([2.,50.,0.]))
        second=candidate.observe(912,metrics([2.,50.,0.]))
        self.assertEqual(first['ipca_decision'],second['ipca_decision'])
        self.assertTrue(second['learning']['deferred'])
        self.assertEqual(old['model'],state(candidate)['model'])

    def test_clean_block_is_accepted_and_matches_unchanged_update(self):
        a=ready()
        b=ready(protect=True)
        for i,row in enumerate(fixture(16,829)):
            left=a.observe(897+i,metrics(row));right=b.observe(897+i,metrics(row))
            self.assertEqual(left['ipca_decision'],right['ipca_decision'])
        self.assertTrue(right['learning']['accepted'])
        self.assertEqual(state(a),state(b))

    def test_explicit_http_error_can_defer_block_without_ipca_alarm(self):
        candidate=ready(protect=True)
        first=candidate.observe(897,metrics([2.,.5,.5]))
        self.assertFalse(first['ipca_decision']['alarm'])
        self.assertTrue(first['http_error_alarm'])
        for i,row in enumerate(fixture(15,829)):last=candidate.observe(898+i,metrics(row))
        self.assertTrue(last['learning']['deferred'])

    def test_recovery_is_provisional_notice_and_never_erases_raw_alarm(self):
        candidate=ready(recovery=True)
        event=candidate.observe(897,metrics([2.,50.,0.]))
        self.assertTrue(event['problem_notice'])
        result=candidate.observe(898,metrics(fixture(1,829)[0]))
        self.assertTrue(result['alarm'])
        self.assertTrue(result['raw_alarm'])
        self.assertEqual(result['notice_kind'],'possible_recovery')
        self.assertFalse(result['problem_notice'])

    def test_http_error_during_faster_response_is_not_recovery(self):
        candidate=ready(recovery=True)
        candidate.observe(897,metrics([2.,50.,0.]))
        result=candidate.observe(898,metrics([2.,.5,.5]))
        self.assertEqual(result['notice_kind'],'problem')
        self.assertTrue(result['problem_notice'])

    def test_still_slow_response_is_not_recovery(self):
        candidate=ready(recovery=True)
        candidate.observe(897,metrics([2.,50.,0.]))
        result=candidate.observe(898,metrics([2.,30.,0.]))
        self.assertEqual(result['notice_kind'],'problem')

    def test_unfit_calibration_is_not_advanced_or_silently_restarted(self):
        candidate=ready(protect=True,recovery=True,rounded=True)
        before=state(candidate)
        result=candidate.observe(897,metrics([2.,50.,0.]))
        self.assertFalse(result['ipca_available'])
        self.assertIsNone(result['ipca_decision'])
        self.assertEqual(state(candidate),before)
        self.assertEqual(candidate.update_counts,dict(attempted=0,accepted=0,deferred=0))

    def test_invalid_input_does_not_change_gate_state(self):
        candidate=ready(protect=True,recovery=True)
        candidate.observe(897,metrics([2.,50.,0.]))
        before=state(candidate)
        saved=(copy.deepcopy(candidate.update_counts),candidate.block_suspect,
               copy.deepcopy(candidate.previous_result),copy.deepcopy(candidate.normal_bounds))
        with self.assertRaises(ValueError):candidate.observe(897,metrics([2.,.5,0.]))
        self.assertEqual(state(candidate),before)
        self.assertEqual(saved,(candidate.update_counts,candidate.block_suspect,
                                candidate.previous_result,candidate.normal_bounds))


if __name__=='__main__':unittest.main(verbosity=2)
