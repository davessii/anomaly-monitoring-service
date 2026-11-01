"""Bounded, deterministic event-time windows with explicit availability times.

    add(record, read_at_ms=...) -> accepted / late / before_start
    advance(now_ms, max_windows=...) -> immutable closed windows

No wall-clock calls, file reads, SQLite writes or IPCA calls. All state belongs
to one source/session. Persistence and exactly-once delivery are later stages.
"""
from dataclasses import dataclass
from .log_schema import (AccessRecord, LogWindow, WINDOW_MS, LATENESS_MS,
                         MAX_PENDING_WINDOWS, MAX_EMIT_WINDOWS,
                         MAX_REQUESTS_PER_WINDOW, MAX_TIME_MS, checked_int)
from .log_parser import PARSE_ERROR_CODES


class AggregationError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


@dataclass(slots=True)
class _Bucket:
    count: int = 0
    errors_5xx: int = 0
    errors_4xx: int = 0
    duration_sum: int = 0
    first_read: int | None = None
    last_read: int | None = None

    def add(self, record, read_at_ms):
        self.count += 1
        self.errors_5xx += 500 <= record.status <= 599
        self.errors_4xx += 400 <= record.status <= 499
        self.duration_sum += record.request_time_ms
        if self.first_read is None:
            self.first_read = read_at_ms
        self.last_read = read_at_ms


class LogAggregator:
    def __init__(self, start_ms, *, max_pending_windows=MAX_PENDING_WINDOWS):
        checked_int(start_ms, 'start_ms', maximum=MAX_TIME_MS - WINDOW_MS - LATENESS_MS - 1)
        if start_ms % WINDOW_MS:
            raise ValueError('start_ms must be the start of a complete UTC-second window')
        checked_int(max_pending_windows, 'max_pending_windows', minimum=1,
                    maximum=MAX_PENDING_WINDOWS)
        self._next_start_ms = start_ms
        self._start_ms = start_ms
        self._last_clock_ms = None
        self._max_pending = max_pending_windows
        self._buckets = {}
        self._requires_restart = False
        self._accepted = self._late = self._before_start = self._emitted = 0
        self._parse_errors = {code: 0 for code in sorted(PARSE_ERROR_CODES)}

    def _check_clock(self, value):
        checked_int(value, 'clock_ms', maximum=MAX_TIME_MS - 1)
        if self._last_clock_ms is not None and value < self._last_clock_ms:
            raise AggregationError('clock_went_backwards')

    @property
    def requires_restart(self):
        return self._requires_restart

    @property
    def pending_windows(self):
        return len(self._buckets)

    def diagnostics(self):
        return dict(accepted_records=self._accepted, late_records=self._late,
                    before_start_records=self._before_start, emitted_windows=self._emitted,
                    pending_windows=len(self._buckets), next_window_start_ms=self._next_start_ms,
                    last_clock_ms=self._last_clock_ms, requires_restart=self._requires_restart,
                    parse_errors=dict(self._parse_errors))

    def add(self, record, *, read_at_ms):
        if type(record) is not AccessRecord:
            raise TypeError('Expected a validated AccessRecord')
        return self._add_record(record.timestamp_ms,record.status,record.request_time_ms,read_at_ms)

    def _add_record(self,timestamp_ms,status,duration,read_at_ms):
        self._check_clock(read_at_ms)
        if timestamp_ms > read_at_ms:
            raise AggregationError('event_is_in_the_future')
        start = (timestamp_ms // WINDOW_MS) * WINDOW_MS
        if timestamp_ms < self._start_ms:
            self._last_clock_ms = read_at_ms
            self._before_start += 1
            return 'before_start'
        # At the deadline the window is closed, even if advance() has not run.
        if start < self._next_start_ms or read_at_ms >= start + WINDOW_MS + LATENESS_MS:
            self._last_clock_ms = read_at_ms
            self._late += 1
            self._requires_restart = True
            return 'late'
        bucket = self._buckets.get(start)
        if bucket is None:
            if len(self._buckets) >= self._max_pending:
                raise AggregationError('pending_capacity_exceeded')
            bucket = _Bucket()
        if bucket.count >= MAX_REQUESTS_PER_WINDOW:
            raise AggregationError('window_capacity_exceeded')
        bucket.count+=1
        bucket.errors_5xx+=500<=status<=599
        bucket.errors_4xx+=400<=status<=499
        bucket.duration_sum+=duration
        if bucket.first_read is None:bucket.first_read=read_at_ms
        bucket.last_read=read_at_ms
        self._buckets[start] = bucket
        self._last_clock_ms = read_at_ms
        self._accepted += 1
        return 'accepted'

    def note_parse_error(self, code, *, read_at_ms):
        """Unknown event time cannot be assigned to a guessed old window.

        Latch session degradation; already returned windows remain immutable.
        A tailer must not report an unfinished normal write as a parse error.
        """
        if code not in self._parse_errors:
            raise ValueError('Unknown parse error code')
        self._check_clock(read_at_ms)
        self._parse_errors[code] += 1
        self._last_clock_ms = read_at_ms
        self._requires_restart = True

    def advance(self, now_ms, *, max_windows=MAX_EMIT_WINDOWS):
        """Close at most max_windows; call again at the same time to drain.

        Empty windows are explicit. A gap/quality failure permanently pauses
        model eligibility for this session, but counters can continue.
        The actual close call time is retained, not the nominal deadline.
        """
        self._check_clock(now_ms)
        checked_int(max_windows, 'max_windows', minimum=1, maximum=MAX_EMIT_WINDOWS)
        self._last_clock_ms = now_ms
        result = []
        while (len(result) < max_windows and
               self._next_start_ms + WINDOW_MS + LATENESS_MS <= now_ms):
            start = self._next_start_ms
            bucket = self._buckets.pop(start, None) or _Bucket()
            if not bucket.count:
                self._requires_restart = True
            result.append(self._window(start,now_ms,bucket))
            self._next_start_ms += WINDOW_MS
            self._emitted += 1
        return result

    def _window(self,start,now_ms,bucket):
        return LogWindow(
                start_ms=start, end_ms=start + WINDOW_MS, finalized_at_ms=now_ms,
                request_count=bucket.count, error_5xx_count=bucket.errors_5xx,
                error_4xx_count=bucket.errors_4xx, duration_sum_ms=bucket.duration_sum,
                first_read_at_ms=bucket.first_read, last_read_at_ms=bucket.last_read,
                session_requires_restart=self._requires_restart)
