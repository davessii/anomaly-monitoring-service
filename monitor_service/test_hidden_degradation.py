"""Pure software fixtures only: no listeners, clients, real records or models."""
import io
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch

from .hidden_degradation import (HiddenPlan,RelativeRule,WorkHandler,absolute_alarm,
                                 bounded_wait,main,protocol,source_health)


class HiddenPlanTests(unittest.TestCase):
    def test_exact_stages_and_constant_test_load(self):
        plan=HiddenPlan()
        for t,name,extra in ((0,'reference',0),(29.999,'reference',0),
                             (30,'hidden_growth',0),(45,'hidden_growth',850000),
                             (60,'recovery',0),(79.999,'recovery',0),(80,'drain',0)):
            self.assertEqual(plan.at(t),dict(phase=name,rate=2,extra_ns=extra))

    def test_growth_is_bounded_and_not_a_fake_total_handler_latency(self):
        values=[HiddenPlan().at(30+i*.01)['extra_ns'] for i in range(3000)]
        self.assertEqual(values,sorted(values))
        self.assertLess(max(values),1_700_000)
        for bad in (-1,float('nan'),float('inf')):
            with self.assertRaises(ValueError):HiddenPlan().at(bad)

    def test_protocol_matches_implemented_schedule_and_criteria(self):
        p=protocol()
        self.assertEqual([r['start_seconds'] for r in p['scenario']],[0,30,60])
        self.assertEqual([r['duration_seconds'] for r in p['scenario']],[30,30,20])
        self.assertEqual(p['scenario'][1]['extra_delay_end_ms'],1.7)
        self.assertEqual(p['success']['deadline_after_hidden_onset_seconds'],15)
        self.assertEqual(p['success']['maximum_reference_signal_seconds'],1)

    def test_relative_rule_uses_fit_low_load_only_not_later_test(self):
        fit=[dict(completed_rps=2,mean_request_ms=.3,server_error_fraction=0) for _ in range(128)]
        fit += [dict(completed_rps=8,mean_request_ms=.8,server_error_fraction=0) for _ in range(128)]
        rule=RelativeRule(fit)
        self.assertEqual(rule.reference_ms,.3)
        self.assertAlmostEqual(rule.threshold_ms,.9)
        fit[0]['mean_request_ms']=2
        self.assertAlmostEqual(rule.threshold_ms,.9)
        self.assertFalse(rule.alarm(rule.threshold_ms))
        self.assertTrue(rule.alarm(rule.threshold_ms+1e-6))

    def test_invalid_reference_never_gets_a_threshold_chosen_from_outcomes(self):
        rows=[dict(completed_rps=2,mean_request_ms=.3,server_error_fraction=0) for _ in range(256)]
        with self.assertRaises(ValueError):RelativeRule(rows[:255])
        rows[0]['server_error_fraction']=1
        with self.assertRaises(ValueError):RelativeRule(rows)
        for row in rows:row.update(completed_rps=8,server_error_fraction=0)
        with self.assertRaises(ValueError):RelativeRule(rows)

    def test_absolute_rule_keeps_strict_ten_ms_requirement(self):
        self.assertFalse(absolute_alarm(2))
        self.assertFalse(absolute_alarm(10))
        self.assertTrue(absolute_alarm(10.001))

    def test_short_wait_uses_a_clock_deadline_and_rejects_large_delays(self):
        now=Mock(side_effect=[0,0,800,1000,1000])
        self.assertEqual(bounded_wait(1000,now),1000)
        for value in (-1,1_700_001,True,.1):
            with self.assertRaises(ValueError):bounded_wait(value)

    def test_handler_logs_measured_duration_not_prescribed_value(self):
        handler=WorkHandler.__new__(WorkHandler)
        handler.headers={'Host':'127.0.0.1:1234'};handler.path='/work';handler.wfile=io.BytesIO()
        handler.send_response=Mock();handler.send_header=Mock();handler.end_headers=Mock()
        access,audit=Mock(),Mock()
        handler.server=SimpleNamespace(server_port=1234,access=access,audit=audit,
            plan_for=lambda _:dict(phase='hidden_growth',rate=2,extra_ns=1_700_000))
        with patch('monitor_service.hidden_degradation.time.perf_counter_ns',side_effect=[100,4200]), \
             patch('monitor_service.hidden_degradation.bounded_wait',return_value=4000):
            handler.do_GET()
        access.record.assert_called_once_with(200,4100)
        record=audit.add.call_args.kwargs
        self.assertEqual(record['prescribed_extra_ns'],1_700_000)
        self.assertEqual(record['measured_handler_ns'],4100)
        # The access log writer receives only status and actual duration.
        self.assertNotIn('phase',access.record.call_args.kwargs)

    def test_handler_rejects_other_paths_without_generating_work(self):
        handler=WorkHandler.__new__(WorkHandler)
        handler.headers={'Host':'127.0.0.1:1234'};handler.path='/other'
        handler.server=SimpleNamespace(server_port=1234,access=Mock())
        handler.send_error=Mock();handler.do_GET()
        handler.server.access.record.assert_not_called()

    def test_default_cli_never_constructs_live_components(self):
        with patch('monitor_service.hidden_degradation.Api') as api, \
             patch('monitor_service.hidden_degradation.WorkApplication') as app, \
             patch('monitor_service.hidden_degradation.AccessLogWriter') as writer, \
             patch('sys.stdout',new_callable=io.StringIO) as stream:
            self.assertEqual(main(['--run','8']),0)
        api.assert_not_called();app.assert_not_called();writer.assert_not_called()
        self.assertEqual(json.loads(stream.getvalue())['real_recordings_started'],0)

    def test_execute_requires_an_explicit_run_number(self):
        with patch('sys.stderr',new_callable=io.StringIO),self.assertRaises(SystemExit):
            main(['--execute'])

    def test_source_configuration_change_and_lost_reader_are_rejected(self):
        api=Mock()
        row=dict(source='s',status='active',worker=dict(running=True,error=None),
                 model=dict(phase='monitoring',ready=True),config={'unchanged':True})
        api.call.return_value=dict(sources=[row])
        self.assertTrue(source_health(api,'s',{'unchanged':True})['ready'])
        with self.assertRaisesRegex(RuntimeError,'configuration'):
            source_health(api,'s',{'different':True})
        row['worker']['running']=False
        with self.assertRaisesRegex(RuntimeError,'Log source'):
            source_health(api,'s',{'unchanged':True})


if __name__=='__main__':unittest.main()
