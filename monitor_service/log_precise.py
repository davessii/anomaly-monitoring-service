"""Version 2 integer-microsecond log windows; legacy version 1 is unchanged."""
from dataclasses import dataclass
from .log_candidate_v2 import PROFILE, PreciseRecord, parse_precise_line, precise_line
from .log_aggregation import LogAggregator
from .log_schema import LogWindow

METRICS_SCHEMA='web_access_metrics_us_v2'

@dataclass(frozen=True,slots=True)
class PreciseLogWindow:
    start_ms:int
    end_ms:int
    finalized_at_ms:int
    request_count:int
    error_5xx_count:int
    error_4xx_count:int
    duration_sum_us:int
    first_read_at_ms:int|None
    last_read_at_ms:int|None
    session_requires_restart:bool

    quality=LogWindow.quality
    eligible_for_model=LogWindow.eligible_for_model
    completed_rps=LogWindow.completed_rps
    server_error_fraction=LogWindow.server_error_fraction
    metrics=LogWindow.metrics
    as_observation=LogWindow.as_observation

    @property
    def mean_request_ms(self):
        return self.duration_sum_us/(1000*self.request_count) if self.request_count else None

    def as_dict(self):
        return dict(profile=PROFILE,metrics_schema=METRICS_SCHEMA,
                    **{name:getattr(self,name) for name in self.__dataclass_fields__},
                    quality=self.quality,eligible_for_model=self.eligible_for_model,metrics=self.metrics())


class PreciseLogAggregator(LogAggregator):
    def add(self,record,*,read_at_ms):
        if type(record) is not PreciseRecord:raise TypeError('Expected precise v2 record')
        return self._add_record(record.timestamp_ms,record.status,record.request_time_us,read_at_ms)

    def _window(self,start,now,bucket):
        return PreciseLogWindow(start,start+1000,now,bucket.count,bucket.errors_5xx,bucket.errors_4xx,
                                bucket.duration_sum,bucket.first_read,bucket.last_read,self._requires_restart)
