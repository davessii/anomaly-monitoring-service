"""Scenario preview only. No collector, network, command or workload entry point."""
from dataclasses import asdict
from .efficiency_schedule import Action,Schedule
from .threshold_guard_protocol import load_protocol


def actions(scenario):
    cfg=load_protocol()
    if scenario not in cfg['collection']['scenarios']:raise ValueError('Scenario must be A, B or C.')
    result=[Action(0,'normal',2,10),Action(60,'load',8,30),Action(90,'load',2,10),
            Action(120,'load',4,30),Action(150,'load',2,10)]
    events=cfg['collection']['scenarios'][scenario]['events']
    if events:
        for event in events:
            result.extend([Action(event['start'],'extra_cpu',0,event['duration']),
                           Action(event['start']+event['duration'],'normal',2,10)])
    else:result.extend([Action(210,'normal',2,10),Action(300,'normal',2,10)])
    return result+[Action(390,'stop',0,0)]


class GuardSchedule(Schedule):
    def __init__(self,scenario):
        super().__init__(2);self.scenario=scenario;self.actions=actions(scenario)


def preview(scenario):
    return dict(scenario=scenario,warmup_rows=896,post_seconds=390,
                post_actions=[asdict(a) for a in actions(scenario)],collection_started=False)
