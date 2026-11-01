"""Isolated Q update gate using an explicit handler-latency requirement.

Reused by log_detector.py for production persistence; not a new quality study.
The latency requirement
is mandatory: this module does not infer it from one known bad fixture or copy
the client's different round-trip limit. Slow-quality flags are NOT added to
raw detector alarms, to isolate the update-gate effect.
"""
import copy
import math

from .log_candidate_v2 import _values
from .log_guard_candidate_v3 import GuardedLogCandidate
from .log_schema import MAX_REQUEST_TIME_MS


class QualityGuardCandidate(GuardedLogCandidate):
    def __init__(self, *, handler_limit_ms, mark_recovery=False, allowed_error_fraction=0.):
        if (type(handler_limit_ms) not in (int,float) or not math.isfinite(handler_limit_ms)
                or not 0<handler_limit_ms<=MAX_REQUEST_TIME_MS):
            raise ValueError('Explicit finite positive handler latency limit is required')
        super().__init__(protect_learning=False,mark_recovery=mark_recovery,
                         allowed_error_fraction=allowed_error_fraction)
        self.handler_limit_ms=float(handler_limit_ms)
        self.block_quality_bad=False

    def observe(self, timestamp, metrics):
        values=_values(metrics)
        slow=values[1]>self.handler_limit_ms
        error=values[2]>self.allowed_error_fraction
        quality_bad=bool(slow or error)
        will_update=(self.detector.ready and self.readiness['eligible']
                     and len(self.detector.pending)==15)
        backup=None
        if will_update:
            backup=(copy.deepcopy(self.detector.model),self.detector.history.copy(),self.detector.rates.copy())
        result=super().observe(timestamp,metrics)
        result['quality_bad']=quality_bad
        result['quality_reasons']=(['handler_time_above_limit'] if slow else [])+(['http_errors_above_limit'] if error else [])
        result['handler_limit_ms']=self.handler_limit_ms
        if result['ipca_decision'] is not None:
            self.block_quality_bad=bool(self.block_quality_bad or quality_bad)
            if will_update:
                # Parent U policy accepted this update. Replace only its update
                # admission; raw scores, alarms, peaks and recovery label stay.
                raw_suspect=result['learning']['suspect_block']
                if self.block_quality_bad:
                    self.detector.model,self.detector.history,self.detector.rates=backup
                    self.update_counts['accepted']-=1
                    self.update_counts['deferred']+=1
                result['learning']=dict(attempted=True,accepted=not self.block_quality_bad,
                                        deferred=self.block_quality_bad,quality_bad_block=self.block_quality_bad,
                                        raw_suspect_block=raw_suspect,policy='quality_requirement')
                self.block_quality_bad=False
        return result
