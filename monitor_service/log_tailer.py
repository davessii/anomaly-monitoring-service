"""Bounded polling of one append-only file; no worker, database or detector.

The caller supplies cycle time and, for live I/O, a clock sampled after reading.
Offsets and partial bytes are IN MEMORY ONLY; durable restart is a later step.
File handles are short lived, so Windows writers may rotate between polls.
"""
from dataclasses import dataclass, replace
from pathlib import Path
import os
import re
import stat
import uuid

from .log_schema import MAX_LINE_BYTES, MAX_TIME_MS, PROFILE, checked_int

POLL_INTERVAL_MS = 200
READ_CHUNK_BYTES = 64 * 1024
MAX_EVENTS_PER_POLL = 256
SAMPLE_BYTES = 128
ANCHOR_BYTES = 64
_IDENTIFIER = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}', re.ASCII)


class TailerError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class FileIdentity:
    device: int
    inode: int


@dataclass(frozen=True, slots=True)
class TailEvent:
    source_id: str
    generation: str
    start_offset: int
    end_offset: int
    read_at_ms: int
    kind: str
    data: bytes | None
    sample: bytes

    @property
    def event_id(self):
        return (self.source_id, self.generation, self.start_offset)


@dataclass(frozen=True, slots=True)
class TailBatch:
    events: tuple[TailEvent, ...]
    bytes_read: int
    bytes_consumed: int
    read_offset: int
    last_complete_offset: int | None
    observed_size: int
    backlog_bytes: int
    waiting_for_lf: bool
    read_completed_at_ms: int


def _identifier(value, name):
    if type(value) is not str or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f'{name}: expected 1..128 ASCII letters, digits, dot, dash or underscore')
    return value


def _identity(info):
    if not stat.S_ISREG(info.st_mode):
        raise TailerError('not_a_regular_file')
    # CPython on Windows exposes the system file index through st_ino.
    # A zero/unavailable identity must not silently fall back to the path.
    if not info.st_ino:
        raise TailerError('file_identity_unavailable')
    return FileIdentity(info.st_dev, info.st_ino)


def _anchor(stream, position):
    start = max(0, position - ANCHOR_BYTES)
    stream.seek(start)
    value = stream.read(position - start)
    if len(value) != position - start:
        raise TailerError('file_shrank')
    return value


class LogTailer:
    def __init__(self, path, *, source_id, mode='live', generation=None,
                 read_chunk_bytes=READ_CHUNK_BYTES,
                 max_events_per_poll=MAX_EVENTS_PER_POLL):
        self.source_id = _identifier(source_id, 'source_id')
        self.generation = _identifier(uuid.uuid4().hex if generation is None else generation,
                                      'generation')
        if mode not in ('live', 'replay'):
            raise ValueError('mode must be live or replay')
        checked_int(read_chunk_bytes, 'read_chunk_bytes', minimum=1, maximum=READ_CHUNK_BYTES)
        checked_int(max_events_per_poll, 'max_events_per_poll', minimum=1,
                    maximum=MAX_EVENTS_PER_POLL)
        self.path = Path(path).absolute()
        self.mode = mode
        self._chunk_size = read_chunk_bytes
        self._event_limit = max_events_per_poll
        self.status = 'active'
        self.reason = None
        self._last_clock_ms = None
        self._buffer = b''
        self._sample = b''
        self._discard = None
        self._total_read = self._total_consumed = self._total_events = 0
        try:
            with self.path.open('rb', buffering=0) as stream:
                info = os.fstat(stream.fileno())
                self.identity = _identity(info)
                path_info = self.path.stat()
                if _identity(path_info) != self.identity:
                    raise TailerError('file_replaced')
                self._read_offset = info.st_size if mode == 'live' else 0
                self._anchor = _anchor(stream, self._read_offset)
                self._line_start = self._read_offset
                self._complete_offset = self._read_offset
                if mode == 'live' and self._read_offset and not self._anchor.endswith(b'\n'):
                    self._discard = 'startup'
                    self._complete_offset = None
                self._observed_size = max(info.st_size, path_info.st_size)
                self._mtime_ns = path_info.st_mtime_ns
        except FileNotFoundError as exc:
            raise TailerError('file_missing') from exc
        except OSError as exc:
            raise TailerError('file_unreadable') from exc

    def diagnostics(self):
        return dict(source_id=self.source_id, generation=self.generation, profile=PROFILE,
                    path=str(self.path), mode=self.mode, status=self.status, reason=self.reason,
                    file_identity=dict(device=self.identity.device, inode=self.identity.inode),
                    read_offset=self._read_offset, last_complete_offset=self._complete_offset,
                    line_start_offset=self._line_start, observed_size=self._observed_size,
                    backlog_bytes=self._observed_size-self._read_offset,
                    buffered_bytes=len(self._buffer), discard_mode=self._discard,
                    sample_bytes=len(self._sample), waiting_for_lf=self._waiting(),
                    last_clock_ms=self._last_clock_ms, total_bytes_read=self._total_read,
                    total_bytes_consumed=self._total_consumed, total_events=self._total_events,
                    durable_checkpoint=False)

    def _waiting(self):
        return bool(self._buffer or self._discard)

    def _fail(self, code):
        self.status = 'needs_restart'
        self.reason = code
        raise TailerError(code)

    def _verify(self, info):
        if _identity(info) != self.identity:
            raise TailerError('file_replaced')
        if info.st_size < self._observed_size:
            raise TailerError('file_shrank')
        if info.st_size == self._observed_size and info.st_mtime_ns != self._mtime_ns:
            raise TailerError('ambiguous_file_change')

    def close(self):
        if self.status == 'active':
            self.status = 'closed'

    def poll(self, now_ms, *, read_clock_ms=None):
        """Live callers supply read_clock_ms; explicit-clock fixtures may omit it.

        A writer can append between the cycle's initial clock sample and read().
        Availability must therefore be sampled after the read/checks, before
        exposing events. Never move an event's own timestamp or allow tolerance.
        """
        if self.status != 'active':
            raise TailerError('source_not_active')
        checked_int(now_ms, 'now_ms', maximum=MAX_TIME_MS-1)
        if self._last_clock_ms is not None and now_ms < self._last_clock_ms:
            self._fail('clock_went_backwards')
        try:
            # Nothing is committed or exposed until the file checks succeed.
            with self.path.open('rb', buffering=0) as stream:
                before = os.fstat(stream.fileno())
                self._verify(before)
                self._verify(self.path.stat())
                if _anchor(stream, self._read_offset) != self._anchor:
                    raise TailerError('file_content_changed')
                stream.seek(self._read_offset)
                data = stream.read(self._chunk_size)
                parsed = self._split(data, now_ms)
                events, used, buffer, sample, discard, line_start, complete = parsed
                next_offset = self._read_offset + used
                next_anchor = _anchor(stream, next_offset)
                # Recheck the previous boundary: appending is allowed, rewriting isn't.
                if _anchor(stream, self._read_offset) != self._anchor:
                    raise TailerError('file_content_changed')
                after = os.fstat(stream.fileno())
                path_after = self.path.stat()
                self._verify(after)
                self._verify(path_after)
                if min(after.st_size, path_after.st_size) < max(before.st_size, next_offset):
                    raise TailerError('file_shrank')
            completed_ms = now_ms if read_clock_ms is None else read_clock_ms()
            checked_int(completed_ms, 'read_completed_at_ms', maximum=MAX_TIME_MS-1)
            if completed_ms < now_ms:
                raise TailerError('clock_went_backwards')
            events = [replace(event, read_at_ms=completed_ms) for event in events]
        except FileNotFoundError:
            self._fail('file_missing')
        except OSError:
            self._fail('file_unreadable')
        except TailerError as exc:
            self._fail(exc.code)

        self._read_offset = next_offset
        self._anchor = next_anchor
        self._buffer, self._sample, self._discard = buffer, sample, discard
        self._line_start, self._complete_offset = line_start, complete
        self._observed_size = max(before.st_size, after.st_size, path_after.st_size)
        self._mtime_ns = path_after.st_mtime_ns
        self._last_clock_ms = completed_ms
        self._total_read += len(data)
        self._total_consumed += used
        self._total_events += len(events)
        return TailBatch(tuple(events), len(data), used, next_offset, complete,
                         self._observed_size, self._observed_size-next_offset, self._waiting(),
                         completed_ms)

    def _split(self, data, now_ms):
        buffer, sample, discard = self._buffer, self._sample, self._discard
        line_start, complete = self._line_start, self._complete_offset
        events = []
        used = 0
        while used < len(data) and len(events) < self._event_limit:
            lf = data.find(b'\n', used)
            end = len(data) if lf < 0 else lf+1
            fragment = data[used:end]
            finished = lf >= 0
            if discard is None:
                size = len(buffer)+len(fragment)
                if size > MAX_LINE_BYTES or (size == MAX_LINE_BYTES and not finished):
                    sample = (buffer[:SAMPLE_BYTES]+fragment[:SAMPLE_BYTES])[:SAMPLE_BYTES]
                    buffer = b''
                    discard = 'oversize'
                else:
                    buffer += fragment
            used = end
            if not finished:
                break
            offset = self._read_offset+used
            kind = {'startup': 'skipped_startup_tail', 'oversize': 'line_too_long'}.get(discard, 'line')
            events.append(TailEvent(self.source_id, self.generation, line_start, offset,
                                    now_ms, kind, buffer if kind == 'line' else None, sample))
            line_start = complete = offset
            buffer, sample, discard = b'', b'', None
        return events, used, buffer, sample, discard, line_start, complete
