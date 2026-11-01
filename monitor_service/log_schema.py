"""Versioned access-log contract; no file IO, model imports or clock reads."""
from dataclasses import dataclass

PROFILE = 'nginx_access_v1'
METRICS_SCHEMA = 'web_access_metrics_v1'
FEATURES = ('completed_rps', 'mean_request_ms', 'server_error_fraction')
WINDOW_MS = 1000
LATENESS_MS = 2000
MAX_LINE_BYTES = 4096  # Includes the terminating LF or CRLF.
MAX_TIME_MS = 100_000_000_000_000  # Matches the Store timestamp upper bound.
MAX_REQUEST_TIME_MS = 1_000_000_000_000
MAX_PENDING_WINDOWS = 8
MAX_EMIT_WINDOWS = 256
MAX_REQUESTS_PER_WINDOW = 1_000_000


def checked_int(value, name, *, minimum=0, maximum=None):
    if type(value) is not int or value < minimum or (maximum is not None and value > maximum):
        raise ValueError(f'{name}: integer outside the supported range')
    return value


@dataclass(frozen=True, slots=True)
class AccessRecord:
    """One completed request, represented exactly in integer milliseconds."""
    timestamp_ms: int
    status: int
    request_time_ms: int

    def __post_init__(self):
        # Leave room for the window end and its lateness deadline.
        checked_int(self.timestamp_ms, 'timestamp_ms', minimum=1,
                    maximum=MAX_TIME_MS - WINDOW_MS - LATENESS_MS - 1)
        checked_int(self.status, 'status', minimum=100, maximum=599)
        checked_int(self.request_time_ms, 'request_time_ms', maximum=MAX_REQUEST_TIME_MS)


class WindowNotEligible(ValueError):
    """Missing or degraded values must never become a numeric model row."""


@dataclass(frozen=True, slots=True)
class LogWindow:
    start_ms: int
    end_ms: int
    finalized_at_ms: int
    request_count: int
    error_5xx_count: int
    error_4xx_count: int
    duration_sum_ms: int
    first_read_at_ms: int | None
    last_read_at_ms: int | None
    session_requires_restart: bool

    @property
    def completed_rps(self):
        return self.request_count / ((self.end_ms - self.start_ms) / 1000)

    @property
    def mean_request_ms(self):
        return self.duration_sum_ms / self.request_count if self.request_count else None

    @property
    def server_error_fraction(self):
        return self.error_5xx_count / self.request_count if self.request_count else None

    @property
    def quality(self):
        if not self.request_count:
            return 'empty'
        return 'degraded' if self.session_requires_restart else 'ok'

    @property
    def eligible_for_model(self):
        return bool(self.request_count and not self.session_requires_restart)

    def metrics(self):
        return dict(zip(FEATURES, (self.completed_rps, self.mean_request_ms,
                                  self.server_error_fraction)))

    def as_observation(self):
        """Prepare the future adapter's payload; does NOT call Store/detector."""
        if not self.eligible_for_model:
            raise WindowNotEligible('Empty or degraded window cannot be sent to the model')
        return {'timestamp': self.end_ms / 1000, 'values': self.metrics()}

    def as_dict(self):
        return dict(profile=PROFILE, metrics_schema=METRICS_SCHEMA,
                    start_ms=self.start_ms, end_ms=self.end_ms,
                    finalized_at_ms=self.finalized_at_ms,
                    request_count=self.request_count, error_5xx_count=self.error_5xx_count,
                    error_4xx_count=self.error_4xx_count, duration_sum_ms=self.duration_sum_ms,
                    first_read_at_ms=self.first_read_at_ms, last_read_at_ms=self.last_read_at_ms,
                    quality=self.quality, session_requires_restart=self.session_requires_restart,
                    eligible_for_model=self.eligible_for_model, metrics=self.metrics())
