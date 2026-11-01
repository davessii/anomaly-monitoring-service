"""Tested precise-log helpers and the original development candidate.

The old v1 parser/writer, stored models and fixed experiments are untouched.
PCA scores/updates are unchanged. Readiness and an explicit HTTP-error rule
are separate from PCA. Production persistence is in log_detector.py, which
reuses these tested helpers. This is not evidence of general detection quality.
"""
from dataclasses import dataclass
import math
import re

from .detector import np
from .immediate import ImmediateDetector
from .log_schema import (FEATURES, MAX_LINE_BYTES, MAX_REQUEST_TIME_MS,
                         MAX_TIME_MS, checked_int)
from .log_parser import LogParseError

PROFILE = 'owned_access_us_v2'
_STAMP = r'(?:0|[1-9][0-9]{0,10})\.[0-9]{3}'
_DURATION = r'(?:0|[1-9][0-9]{0,12})\.[0-9]{6}'
_LINE = re.compile(r'v2 ts=(?P<ts>'+_STAMP+r') status=(?P<status>[0-9]{3}) rt=(?P<rt>'+_DURATION+r')', re.ASCII)


@dataclass(frozen=True, slots=True)
class PreciseRecord:
    timestamp_ms: int
    status: int
    request_time_us: int

    def __post_init__(self):
        checked_int(self.timestamp_ms, 'timestamp_ms', minimum=1, maximum=MAX_TIME_MS-3001)
        checked_int(self.status, 'status', minimum=100, maximum=599)
        checked_int(self.request_time_us, 'request_time_us', maximum=MAX_REQUEST_TIME_MS*1000)


def precise_line(timestamp_ms, status, duration_ns):
    """Owned-app v2 only: preserve integer microseconds, never invent them."""
    checked_int(duration_ns, 'duration_ns', maximum=1_000_000_000_000_000_000)
    us = duration_ns//1000
    # As in v1, 000 denotes unknown delivery, and must fail at parsing.
    checked_int(status, 'status', maximum=599)
    if status and status < 100:
        raise ValueError('Expected 100..599 or 000')
    PreciseRecord(timestamp_ms, status or 100, us)
    return (f'v2 ts={timestamp_ms//1000}.{timestamp_ms%1000:03d} '
            f'status={status:03d} rt={us//1_000_000}.{us%1_000_000:06d}\n').encode('ascii')


def parse_precise_line(line):
    if type(line) is not bytes:
        raise LogParseError('invalid_type')
    if len(line) > MAX_LINE_BYTES:
        raise LogParseError('line_too_long')
    if not line.endswith(b'\n'):
        raise LogParseError('incomplete_line')
    body = line[:-1]
    if body.endswith(b'\r'):
        body = body[:-1]
    try:
        text = body.decode('utf-8', errors='strict')
    except UnicodeDecodeError as exc:
        raise LogParseError('invalid_utf8') from exc
    match = _LINE.fullmatch(text)
    if not match:
        raise LogParseError('format_mismatch')
    ts_sec, ts_ms = match['ts'].split('.')
    rt_sec, rt_us = match['rt'].split('.')
    try:
        return PreciseRecord(int(ts_sec)*1000+int(ts_ms), int(match['status']),
                             int(rt_sec)*1_000_000+int(rt_us))
    except ValueError as exc:
        raise LogParseError('value_out_of_range') from exc


class PreciseSecond:
    """Bounded sums for one explicit second. No tailing/closing/checkpoint here."""
    def __init__(self, start_ms):
        checked_int(start_ms, 'start_ms', maximum=MAX_TIME_MS-3001)
        if start_ms % 1000:
            raise ValueError('Expected whole-second start')
        self.start_ms = start_ms
        self.count = self.duration_us = self.errors = 0

    def add(self, record):
        if type(record) is not PreciseRecord:
            raise TypeError('Expected precise record')
        if not self.start_ms <= record.timestamp_ms < self.start_ms+1000:
            raise ValueError('Record outside this second')
        if self.count >= 1_000_000:
            raise ValueError('Second request capacity exceeded')
        self.count += 1
        self.duration_us += record.request_time_us
        self.errors += 500 <= record.status <= 599

    def metrics(self):
        # Keep the sum integer until final division; no per-request ms truncation.
        return dict(completed_rps=float(self.count),
                    mean_request_ms=self.duration_us/(1000*self.count) if self.count else None,
                    server_error_fraction=self.errors/self.count if self.count else None)


def calibration_readiness(detector):
    """Identify a numerically degenerate Q calibration, without changing it.

    This is a readiness guard, not a learned threshold or claimed false-alarm
    bound. A constant error fraction alone does NOT invalidate a model.
    """
    if tuple(detector.features) != FEATURES:
        raise ValueError('Expected exact three log features in fixed order')
    if not detector.ready:
        return dict(eligible=False, reasons=['warmup_incomplete'])
    history = detector.history
    q99 = float(np.quantile(detector.model.score(history), .99))
    magnitude = max(1., float(np.quantile(np.mean(history**2, axis=1), .99)))
    roundoff_bound = 64*np.finfo(float).eps*magnitude
    reasons = []
    if q99 <= roundoff_bound:
        reasons.append('reference_error_at_roundoff')
    # 1e-12 is the existing detector's exact fallback for empty Q calibration;
    # it must not silently become a meaningful calibration at a new interface.
    if detector.channel_scales[0] <= 1e-12:
        reasons.append('calibration_error_at_numeric_floor')
    return dict(eligible=not reasons, reasons=reasons, reference_q99=q99,
                roundoff_bound=roundoff_bound,
                q_channel_scale=float(detector.channel_scales[0]),
                constant_reference_features=[detector.features[j] for j in range(len(FEATURES))
                                             if np.ptp(history[:,j]) == 0])


def _values(metrics):
    if type(metrics) is not dict or set(metrics) != set(FEATURES):
        raise ValueError('Expected exact log metrics without missing values')
    values = [metrics[f] for f in FEATURES]
    if any(type(v) not in (int,float) or not math.isfinite(v) for v in values):
        raise ValueError('Expected finite numeric metrics')
    if not (0 < values[0] <= 1_000_000 and 0 <= values[1] <= MAX_REQUEST_TIME_MS
            and 0 <= values[2] <= 1):
        raise ValueError('Metrics outside supported ranges')
    return values


class CheckedLogCandidate:
    """In-memory candidate, no database or live-service registration.

    HTTP threshold 0 is an explicit requirement of the error-free owned test
    app, NOT a universal production SLO. Its alerts are labelled separately.
    """
    def __init__(self, allowed_error_fraction=0.):
        if (type(allowed_error_fraction) not in (int,float) or
                not math.isfinite(allowed_error_fraction) or not 0 <= allowed_error_fraction <= 1):
            raise ValueError('Error fraction limit must be finite and within 0..1')
        self.allowed_error_fraction = allowed_error_fraction
        self.detector = ImmediateDetector(FEATURES, components=1)
        self.readiness = dict(eligible=False, reasons=['warmup_incomplete'])
        self.last_timestamp = None

    def observe(self, timestamp, metrics):
        if (type(timestamp) not in (int,float) or not math.isfinite(timestamp) or timestamp <= 0 or
                self.last_timestamp is not None and timestamp <= self.last_timestamp):
            raise ValueError('Expected positive increasing timestamp')
        values = _values(metrics)
        result = []
        # A failed calibration is not retried/reset silently. Its own completed
        # state is retained for diagnosis, and explicit error rules still work.
        if not self.detector.ready or self.readiness['eligible']:
            was_ready = self.detector.ready
            result = self.detector.observe(timestamp, values)
            if not was_ready and self.detector.ready:
                self.readiness = calibration_readiness(self.detector)
        self.last_timestamp = timestamp
        ipca_alarm = bool(result and result[0]['alarm'])
        http_error = values[2] > self.allowed_error_fraction
        sources = (['ipca'] if ipca_alarm else [])+(['http_error_rule'] if http_error else [])
        return dict(timestamp=timestamp, alarm=bool(sources), signal_sources=sources,
                    ipca_available=self.readiness['eligible'],
                    ipca_decision=result[0] if result else None,
                    http_error_alarm=http_error, readiness=dict(self.readiness))
