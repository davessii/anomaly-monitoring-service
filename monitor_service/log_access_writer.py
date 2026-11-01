"""Owned application's access-log writer. No detector or control labels here."""
from pathlib import Path
import threading
import time
from .log_schema import AccessRecord, checked_int, MAX_TIME_MS


def format_line(timestamp_ms, status, duration_ns):
    checked_int(timestamp_ms, 'timestamp_ms', minimum=1, maximum=MAX_TIME_MS-3001)
    checked_int(status, 'status', maximum=599)
    if status != 0 and status < 100:
        raise ValueError('HTTP status must be 100..599, or 000 for incomplete delivery')
    checked_int(duration_ns, 'duration_ns', maximum=1_000_000_000_000_000_000)
    duration_ms = duration_ns//1_000_000
    AccessRecord(timestamp_ms, status or 100, duration_ms)
    return (f'v1 ts={timestamp_ms//1000}.{timestamp_ms%1000:03d} '
            f'status={status:03d} rt={duration_ms//1000}.{duration_ms%1000:03d}\n').encode('ascii')


class AccessLogWriter:
    def __init__(self, path, *, clock_ms=None,profile='nginx_access_v1'):
        if profile not in ('nginx_access_v1','owned_access_us_v2'):raise ValueError('Unknown access profile')
        self.profile=profile
        self.path = Path(path).absolute()
        self.clock_ms = clock_ms or (lambda: time.time_ns()//1_000_000)
        # Fresh file per app run; never truncate or silently append an old run.
        self.stream = self.path.open('xb', buffering=0)
        self.lock = threading.Lock()
        self.written = self.unknown_status = 0
        self.error = None
        self.closed = False

    def record(self, status, duration_ns):
        with self.lock:
            if self.error or self.closed:
                return False
            try:
                if self.profile=='owned_access_us_v2':
                    from .log_precise import precise_line
                    data=precise_line(self.clock_ms(),status,duration_ns)
                else:data = format_line(self.clock_ms(), status, duration_ns)
                if self.stream.write(data) != len(data):
                    raise OSError('short access-log write')
            except (OSError, ValueError) as exc:
                self.error = type(exc).__name__  # No paths/tokens/free-form labels in metrics.
                return False
            self.written += 1
            self.unknown_status += status == 0
            return True

    def diagnostics(self):
        with self.lock:
            return dict(profile=self.profile, filename=self.path.name, written_records=self.written,
                        unknown_status_records=self.unknown_status, error=self.error, closed=self.closed,
                        duration_scope='application_handler_to_response_flush', precision='integer_us_floor' if self.profile=='owned_access_us_v2' else 'integer_ms_floor')

    def close(self):
        with self.lock:
            if not self.closed:
                self.stream.close()
                self.closed = True
