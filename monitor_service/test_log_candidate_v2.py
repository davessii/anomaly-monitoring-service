"""Fixed short software fixtures, not confirmation data or a quality study."""
import json
import unittest

from .detector import np
from .immediate import ImmediateDetector
from .log_candidate_v2 import (PreciseRecord, PreciseSecond, precise_line,
                               parse_precise_line, calibration_readiness, CheckedLogCandidate)
from .log_parser import parse_line, LogParseError
from .log_schema import FEATURES
from .test_log_tailer import BASE


def fixture(length, seed):
    rng = np.random.default_rng(seed)
    rps = rng.choice([1.,2.,3.], size=length, p=[.1,.8,.1])
    us = rng.integers(250,751,size=length)
    return np.column_stack((rps,us/1000,np.zeros(length)))


def metrics(row):
    return dict(zip(FEATURES,row.tolist() if isinstance(row,np.ndarray) else row))


def prepared(*, rounded=False, limit=0.):
    candidate = CheckedLogCandidate(limit)
    rows = fixture(896,471)
    if rounded:
        rows[:,1] = 0
    for i,row in enumerate(rows):
        candidate.observe(i+1,metrics(row))
    return candidate


class PreciseLogTests(unittest.TestCase):
    def test_submillisecond_integer_precision_is_preserved(self):
        line = precise_line(BASE+123,200,250_999)
        self.assertEqual(line,b'v2 ts=1792490400.123 status=200 rt=0.000250\n')
        row = parse_precise_line(line)
        self.assertEqual((row.timestamp_ms,row.status,row.request_time_us),(BASE+123,200,250))

    def test_new_and_old_formats_cannot_be_confused(self):
        v2 = precise_line(BASE+100,200,250_000)
        with self.assertRaises(LogParseError):
            parse_line(v2)
        with self.assertRaises(LogParseError):
            parse_precise_line(b'v1 ts=1792490400.100 status=200 rt=0.000\n')

    def test_exact_sum_precedes_conversion_to_milliseconds(self):
        second = PreciseSecond(BASE)
        second.add(parse_precise_line(precise_line(BASE+100,200,250_000)))
        second.add(parse_precise_line(precise_line(BASE+200,503,750_000)))
        self.assertEqual(second.duration_us,1000)
        self.assertEqual(second.metrics(),dict(completed_rps=2.,mean_request_ms=.5,
                                               server_error_fraction=.5))

    def test_empty_second_keeps_missing_values(self):
        self.assertEqual(PreciseSecond(BASE).metrics(),dict(completed_rps=0.,mean_request_ms=None,
                                                          server_error_fraction=None))

    def test_second_is_half_open_and_rejects_legacy_type(self):
        second = PreciseSecond(BASE)
        second.add(PreciseRecord(BASE,200,1))
        with self.assertRaises(ValueError):
            second.add(PreciseRecord(BASE+1000,200,1))
        with self.assertRaises(TypeError):
            second.add(parse_line(b'v1 ts=1792490400.100 status=200 rt=0.000\n'))

    def test_strict_format_units_status_types_and_bounds(self):
        good = precise_line(BASE+100,200,1)
        bad = [good[:-1],good.replace(b'0.000000',b'0.000'),good+b'extra',
               good.replace(b'status=200',b'status=000'),good.replace(b'v2 ',b'v3 '),
               good.replace(b'rt=0.000000',b'rt=-0.000001'),b'\xff\n',None]
        for value in bad:
            with self.subTest(value=value),self.assertRaises(LogParseError):
                parse_precise_line(value)
        for args in ((BASE,99,1),(BASE,200,-1),(BASE,200,1.2),(True,200,1),(BASE,True,1)):
            with self.subTest(args=args),self.assertRaises(ValueError):
                precise_line(*args)

    def test_crlf_and_unknown_delivery_are_explicit(self):
        value=precise_line(BASE+100,200,1000)
        self.assertEqual(parse_precise_line(value[:-1]+b'\r\n').request_time_us,1)
        with self.assertRaises(LogParseError):
            parse_precise_line(precise_line(BASE+100,0,1000))


class CheckedLogTests(unittest.TestCase):
    def test_old_rounded_calibration_is_rejected_without_silently_resetting_it(self):
        candidate=prepared(rounded=True)
        self.assertFalse(candidate.readiness['eligible'])
        self.assertIn('reference_error_at_roundoff',candidate.readiness['reasons'])
        self.assertIn('calibration_error_at_numeric_floor',candidate.readiness['reasons'])
        before=candidate.detector.to_state()
        for i in range(32):
            result=candidate.observe(897+i,metrics([2.,.5,0.]))
            self.assertFalse(result['ipca_available'])
            self.assertIsNone(result['ipca_decision'])
        self.assertEqual(before,candidate.detector.to_state())

    def test_precise_calibration_allows_constant_error_fraction(self):
        candidate=prepared()
        self.assertTrue(candidate.readiness['eligible'])
        self.assertIn('server_error_fraction',candidate.readiness['constant_reference_features'])
        self.assertGreater(candidate.detector.channel_scales[0],1e-12)

    def test_readiness_check_does_not_change_state_or_claim_warmup_ready(self):
        detector=ImmediateDetector(FEATURES,components=1)
        state=detector.to_state()
        self.assertEqual(calibration_readiness(detector),dict(eligible=False,reasons=['warmup_incomplete']))
        self.assertEqual(state,detector.to_state())
        candidate=prepared()
        state=candidate.detector.to_state()
        self.assertTrue(calibration_readiness(candidate.detector)['eligible'])
        self.assertEqual(state,candidate.detector.to_state())

    def test_ordinary_submillisecond_variation_does_not_cause_alert_storm(self):
        candidate=prepared()
        outputs=[candidate.observe(897+i,metrics(row)) for i,row in enumerate(fixture(128,829))]
        self.assertTrue(all(r['ipca_available'] for r in outputs))
        self.assertEqual(sum(r['alarm'] for r in outputs),0)

    def test_obvious_latency_is_ipca_alert_and_not_http_error(self):
        candidate=prepared()
        result=candidate.observe(897,metrics([2.,50.,0.]))
        self.assertTrue(result['alarm'])
        self.assertEqual(result['signal_sources'],['ipca'])
        self.assertFalse(result['http_error_alarm'])
        # Expose the test's finite recovery period rather than claim zero cost.
        recovery=[candidate.observe(898+i,metrics(row)) for i,row in enumerate(fixture(128,829))]
        self.assertEqual(sum(r['alarm'] for r in recovery[-32:]),0)

    def test_error_rule_is_explicit_even_when_ipca_misses_single_error_second(self):
        candidate=prepared()
        result=candidate.observe(897,metrics([2.,.5,.5]))
        self.assertFalse(result['ipca_decision']['alarm'])
        self.assertTrue(result['http_error_alarm'])
        self.assertEqual(result['signal_sources'],['http_error_rule'])
        recovery=[candidate.observe(898+i,metrics(row)) for i,row in enumerate(fixture(128,829))]
        self.assertEqual(sum(r['alarm'] for r in recovery[-32:]),0)

    def test_errors_are_not_hidden_by_failed_calibration_or_warmup(self):
        for candidate,timestamp in ((CheckedLogCandidate(),1),(prepared(rounded=True),897)):
            result=candidate.observe(timestamp,metrics([2.,.5,.5]))
            self.assertFalse(result['ipca_available'])
            self.assertTrue(result['http_error_alarm'])
            self.assertEqual(result['signal_sources'],['http_error_rule'])

    def test_error_fraction_requirement_is_configurable_and_boundary_is_strict(self):
        candidate=CheckedLogCandidate(.25)
        self.assertFalse(candidate.observe(1,metrics([4.,.5,.25]))['http_error_alarm'])
        self.assertTrue(candidate.observe(2,metrics([4.,.5,.5]))['http_error_alarm'])

    def test_bad_inputs_and_out_of_order_time_cannot_advance_state(self):
        candidate=CheckedLogCandidate()
        candidate.observe(1,metrics([2.,.5,0.]))
        state=candidate.detector.to_state()
        bad=[(1,metrics([2.,.5,0.])),(2,metrics([0.,None,None])),(2,metrics([2.,-.5,0.])),
             (2,metrics([2.,.5,2.])),(2,metrics([True,.5,0.])),(2,{'completed_rps':2.}),
             (float('nan'),metrics([2.,.5,0.]))]
        for timestamp,value in bad:
            with self.subTest(timestamp=timestamp,value=value),self.assertRaises(ValueError):
                candidate.observe(timestamp,value)
            self.assertEqual(state,candidate.detector.to_state())
            self.assertEqual(candidate.last_timestamp,1)

    def test_available_ipca_decisions_and_updates_remain_identical_to_original(self):
        candidate=prepared()
        # Production persistence uses independent JSON objects. to_state()
        # alone contains list references, so it is not an isolated snapshot.
        original=ImmediateDetector.from_state(json.loads(json.dumps(candidate.detector.to_state())))
        for i,row in enumerate(fixture(64,829)):
            expected=original.observe(897+i,row)[0]
            actual=candidate.observe(897+i,metrics(row))
            self.assertEqual(actual['ipca_decision'],expected)
        self.assertEqual(original.to_state(),candidate.detector.to_state())


if __name__=='__main__':unittest.main(verbosity=2)
