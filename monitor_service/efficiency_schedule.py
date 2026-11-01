"""Deterministic pilot commands, no I/O and no workload on import."""
from dataclasses import dataclass
import math

WARMUP = 896
POST_SECONDS = 390


@dataclass(frozen=True)
class Action:
    at: float
    kind: str
    value: float
    seconds: float


def post_schedule(order):
    if type(order) is not int or order not in (1, 2):
        raise ValueError('Order must be 1 or 2.')
    first, second = (4, 8) if order == 1 else (8, 4)
    short, long = (10, 30) if order == 1 else (30, 10)
    start_second = 210 + short + 60
    return [Action(0, 'normal', 2, 10), Action(60, 'load', first, 30),
            Action(90, 'load', 2, 10), Action(120, 'load', second, 30),
            Action(150, 'load', 2, 10), Action(210, 'extra_cpu', 0, short),
            Action(210 + short, 'normal', 2, 10), Action(start_second, 'extra_cpu', 0, long),
            Action(start_second + long, 'normal', 2, 10), Action(390, 'stop', 0, 0)]


class Schedule:
    """Observe progress, never alarm/outcome data. Abort on missed milestones."""
    def __init__(self, order):
        self.actions = post_schedule(order)
        self.index = 0
        self.training_group = None
        self.post_origin = None
        self.last_time = None
        self.last_count = 0
        self.last_training_command = None
        self.finished = False

    def tick(self, samples, phase, now):
        if type(samples) is not int or samples < self.last_count:
            raise ValueError('Sample progress must not decrease.')
        if not math.isfinite(now) or (self.last_time is not None and now < self.last_time):
            raise ValueError('Monotonic time must not decrease.')
        self.last_time = now
        self.last_count = samples
        if self.finished:
            return []
        if self.post_origin is None and samples < WARMUP:
            if phase != 'warmup':
                raise ValueError('Unexpected early end of learning.')
            group = samples // 32
            if self.training_group is not None and group > self.training_group + 1:
                raise ValueError('Training load milestone missed.')
            # TTL is a safety lease, renewed before 120s if progress is slow.
            if group != self.training_group or self.last_training_command is None or now - self.last_training_command >= 60:
                self.training_group = group
                self.last_training_command = now
                return [Action(now, 'load', (2, 4, 8)[group % 3], 120)]
            return []
        if self.post_origin is None:
            if samples != WARMUP or phase != 'monitoring':
                raise ValueError('Exact 896-row transition was missed; do not compress schedule.')
            self.post_origin = now
        elif phase != 'monitoring':
            raise ValueError('Monitoring phase unexpectedly changed.')
        elapsed = now - self.post_origin
        if self.index >= len(self.actions) or elapsed < self.actions[self.index].at:
            return []
        action = self.actions[self.index]
        if elapsed - action.at > 1:
            raise ValueError('Scheduled command is more than one second late.')
        self.index += 1
        if action.kind == 'stop':
            self.finished = True
        return [action]


def preview(order):
    return {'order': order, 'warmup_rows': WARMUP, 'training_load_cycle_rps': [2, 4, 8],
            'training_load_rows': 32, 'post_seconds': POST_SECONDS,
            'post_actions': [a.__dict__ for a in post_schedule(order)],
            'scope': 'Development only; not started by preview.'}
