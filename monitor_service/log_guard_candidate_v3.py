"""Original development guards, reused by the persisted log_detector adapter.

G protects the model AND its reference/rate histories as a bundle. R only adds
a provisional notice label: it never changes raw alarms, scores or learning.
See work/log-guard-check-spec-v01.md. No alternative update acceptance policy
is selected from results here.
"""
import copy

from .log_candidate_v2 import CheckedLogCandidate
from .log_schema import FEATURES


class GuardedLogCandidate(CheckedLogCandidate):
    def __init__(self, *, protect_learning=False, mark_recovery=False, allowed_error_fraction=0.):
        if type(protect_learning) is not bool or type(mark_recovery) is not bool:
            raise ValueError('Expected boolean candidate factors')
        super().__init__(allowed_error_fraction)
        self.protect_learning=protect_learning
        self.mark_recovery=mark_recovery
        self.normal_bounds=None
        self.previous_metrics=None
        self.previous_result=None
        self.block_suspect=False
        self.update_counts=dict(attempted=0,accepted=0,deferred=0)

    def observe(self, timestamp, metrics):
        was_ready=self.detector.ready
        will_update=(was_ready and self.readiness['eligible'] and len(self.detector.pending)==15)
        backup=None
        if will_update and self.protect_learning:
            backup=(copy.deepcopy(self.detector.model),self.detector.history.copy(),self.detector.rates.copy())
        result=super().observe(timestamp,metrics)
        if not was_ready:
            if self.normal_bounds is None:
                self.normal_bounds={f:[metrics[f],metrics[f]] for f in FEATURES}
            else:
                for f in FEATURES:
                    self.normal_bounds[f][0]=min(self.normal_bounds[f][0],metrics[f])
                    self.normal_bounds[f][1]=max(self.normal_bounds[f][1],metrics[f])
        ipca=result['ipca_decision']
        report=dict(attempted=False,accepted=False,deferred=False)
        if ipca is not None:
            suspect=bool(ipca['base_alarm'] or result['http_error_alarm'])
            self.block_suspect=bool(self.block_suspect or suspect)
            if will_update:
                deferred=bool(self.protect_learning and self.block_suspect)
                if deferred:
                    # Keep the actual last row for next delta. Raw results,
                    # block peak/cut records and pending reset remain intact.
                    self.detector.model,self.detector.history,self.detector.rates=backup
                self.update_counts['attempted']+=1
                self.update_counts['deferred' if deferred else 'accepted']+=1
                report=dict(attempted=True,accepted=not deferred,deferred=deferred,
                            suspect_block=self.block_suspect)
                self.block_suspect=False
        raw_alarm=result['alarm']
        kind='problem' if raw_alarm else 'ordinary'
        previous=self.previous_result
        if (self.mark_recovery and raw_alarm and ipca is not None and not ipca['base_alarm']
                and not result['http_error_alarm'] and previous is not None
                and previous['ipca_decision'] is not None and previous['ipca_decision']['base_alarm']
                and previous['ipca_decision']['largest_residual_features'][0]=='mean_request_ms'
                and self.normal_bounds is not None):
            old=self.previous_metrics
            lower,upper=self.normal_bounds['mean_request_ms']
            low_rps,high_rps=self.normal_bounds['completed_rps']
            if (old['mean_request_ms']>upper and lower<=metrics['mean_request_ms']<=upper
                    and metrics['mean_request_ms']<old['mean_request_ms']
                    and low_rps<=metrics['completed_rps']<=high_rps):
                kind='possible_recovery'
        result.update(raw_alarm=raw_alarm,notice_kind=kind,problem_notice=kind=='problem',
                      learning=report)
        self.previous_metrics=dict(metrics)
        self.previous_result=result
        return result
