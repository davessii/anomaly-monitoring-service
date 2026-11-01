"""Control-flow fixtures only; no detector training or real requests."""
import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from .warmup_load import NormalLoadPlan
from .validation_multiload import MultiLoadScenario, load_protocol, main
from .validation_scenario import Events, ROOT, schedule
from .test_validation_scenario import FakeApi, FakeClock, FakeEvidence


class WarmupTests(unittest.TestCase):
    def setUp(self):
        self.protocol=load_protocol()
        self.clock=FakeClock()
        self.api=FakeApi(self.clock,self.protocol)
        root=ROOT/'work/multiload-unit-tests'
        root.mkdir(exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(dir=root)
        self.addCleanup(self.temp.cleanup)
        self.folder=Path(self.temp.name)

    def runner(self):
        runner=MultiLoadScenario(self.protocol,6,self.api,FakeEvidence(),self.folder,
                                clock=self.clock,check_bundle=lambda:'test-seal')
        runner.events=Events(self.folder/'events.jsonl',self.clock)
        self.addCleanup(runner.events.close)
        return runner

    def test_exact_plan_covers_both_levels_in_each_original_training_part(self):
        plan=NormalLoadPlan()
        self.assertEqual(plan.duration,896)
        for begin,end in ((0,256),(256,384),(384,896)):
            rates=[plan.at(t)['rate'] for t in range(begin,end)]
            self.assertEqual(rates.count(2),rates.count(8))
        self.assertEqual(plan.at(63.999)['rate'],2)
        self.assertEqual(plan.at(64)['rate'],8)
        self.assertEqual(plan.at(896)['rate'],2)
        self.assertTrue(plan.at(896)['curriculum_complete'])

    def test_no_early_exit_even_if_fixture_reports_ready_immediately(self):
        runner=self.runner();runner.warmup()
        loads=[body for path,body,_ in self.api.calls if path=='/api/bench/load']
        self.assertEqual([b['rate'] for b in loads],[2,8]*7+[2])
        self.assertTrue(all(b['seconds']==120 for b in loads))
        self.assertGreaterEqual(self.clock.now,896)
        self.assertFalse(any(path=='/api/bench/control' for path,_,_ in self.api.calls))
        self.assertTrue((self.folder/'model_before.json').exists())

    def test_timeout_preserves_one_attempt_without_model_or_parameter_reset(self):
        self.api.warmup_never_ready=True
        runner=self.runner()
        with self.assertRaises(TimeoutError):runner.warmup()
        self.assertEqual(sum(path=='/api/bench/start' for path,_,_ in self.api.calls),1)
        self.assertFalse((self.folder/'model_before.json').exists())

    def test_skipped_slot_is_technical_failure_not_rescheduled_curriculum(self):
        runner=self.runner()
        base=runner.health
        def skip():
            result=base()
            self.clock.sleep(130)
            return result
        runner.health=skip
        with self.assertRaisesRegex(RuntimeError,'skipped'):runner.warmup()

    def test_scenario_6_and_7_equal_old_run_1_and_settings_unchanged(self):
        from .validation_scenario import load_protocol as old_protocol
        old=old_protocol()
        for number in (6,7):
            self.assertEqual(schedule(self.protocol,number),schedule(old,1))
        for name in ('model_config','source_config','generator','runtime_versions'):
            self.assertEqual(self.protocol[name],old[name])

    def test_cli_without_execute_does_not_construct_api(self):
        with patch('monitor_service.validation_multiload.verify_bundle',return_value='fixture'), \
             patch('monitor_service.validation_multiload.Api') as api, \
             patch('sys.stdout',new_callable=io.StringIO) as stream:
            self.assertEqual(main(['--run','6']),0)
        api.assert_not_called()
        self.assertEqual(json.loads(stream.getvalue())['real_recordings_started'],0)


if __name__=='__main__':unittest.main()
