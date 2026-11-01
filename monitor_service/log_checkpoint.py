"""Versioned, bounded JSON state for the log reader and aggregate buckets.

This serializer is intentionally coupled to the v1 private fields. A format
change needs a new version; persisted objects are never unpickled/executed.
"""
from pathlib import Path
from .log_tailer import LogTailer, FileIdentity, _identifier, ANCHOR_BYTES, SAMPLE_BYTES
from .log_aggregation import LogAggregator, _Bucket
from .log_schema import (MAX_TIME_MS, MAX_LINE_BYTES, MAX_REQUEST_TIME_MS,
                         MAX_REQUESTS_PER_WINDOW, LogWindow, checked_int)
from .log_parser import PARSE_ERROR_CODES
from .log_precise import PreciseLogAggregator,PreciseLogWindow,PROFILE as PRECISE_PROFILE

VERSION = 1
MAX_COUNTER = 2**63-1


def integer(value, minimum=0, maximum=MAX_COUNTER):
    return checked_int(value, 'checkpoint integer', minimum=minimum, maximum=maximum)


def boolean(value):
    if type(value) is not bool:
        raise ValueError('Checkpoint boolean expected')
    return value


def clock(value):
    return None if value is None else integer(value, maximum=MAX_TIME_MS-1)


def raw(value, limit):
    if type(value) is not str or len(value) > limit*2:
        raise ValueError('Invalid byte buffer')
    result = bytes.fromhex(value)
    if result.hex() != value or len(result) > limit:
        raise ValueError('Noncanonical or oversized byte buffer')
    return result


def pack(reader, aggregate):
    r = reader.__dict__.copy()
    r['path'] = str(reader.path)
    r['identity'] = dict(device=reader.identity.device, inode=reader.identity.inode)
    for key in ('_buffer', '_sample', '_anchor'):
        r[key] = r[key].hex()
    a = aggregate.__dict__.copy()
    a['_buckets'] = [dict(start=start, count=b.count, errors_5xx=b.errors_5xx,
                          errors_4xx=b.errors_4xx, duration_sum=b.duration_sum,
                          first_read=b.first_read, last_read=b.last_read)
                     for start, b in sorted(aggregate._buckets.items())]
    a['_parse_errors'] = dict(a['_parse_errors'])
    return dict(version=2 if isinstance(aggregate,PreciseLogAggregator) else VERSION, reader=r, aggregate=a)


def unpack(state):
    if (not isinstance(state, dict) or set(state) != {'version', 'reader', 'aggregate'} or
            type(state['version']) is not int or state['version'] not in (1,2)):
        raise ValueError('Unsupported checkpoint version/shape')
    r, a = dict(state['reader']), dict(state['aggregate'])
    expected_r = {'source_id', 'generation', 'path', 'mode', '_chunk_size', '_event_limit',
                  'status', 'reason', '_last_clock_ms', '_buffer', '_sample', '_discard',
                  '_total_read', '_total_consumed', '_total_events', 'identity',
                  '_read_offset', '_anchor', '_line_start', '_complete_offset',
                  '_observed_size', '_mtime_ns'}
    expected_a = {'_next_start_ms', '_start_ms', '_last_clock_ms', '_max_pending',
                  '_buckets', '_requires_restart', '_accepted', '_late', '_before_start',
                  '_emitted', '_parse_errors'}
    if set(r) != expected_r or set(a) != expected_a:
        raise ValueError('Unknown/missing checkpoint fields')
    _identifier(r['source_id'], 'source_id')
    _identifier(r['generation'], 'generation')
    if type(r['path']) is not str or not Path(r['path']).is_absolute():
        raise ValueError('Checkpoint path must be absolute')
    r['path'] = Path(r['path'])
    if r['mode'] not in ('live', 'replay') or r['status'] not in ('active', 'closed', 'needs_restart'):
        raise ValueError('Invalid reader mode/status')
    if (r['status'] == 'needs_restart') != (type(r['reason']) is str and 1 <= len(r['reason']) <= 64):
        raise ValueError('Invalid reader stop reason')
    if r['status'] != 'needs_restart' and r['reason'] is not None:
        raise ValueError('Unexpected reader stop reason')
    integer(r['_chunk_size'], 1, 65536)
    integer(r['_event_limit'], 1, 256)
    for key in ('_total_read', '_total_consumed', '_total_events', '_read_offset',
                '_line_start', '_observed_size', '_mtime_ns'):
        integer(r[key])
    complete = r['_complete_offset']
    if complete is not None:
        integer(complete, maximum=r['_line_start'])
    clock(r['_last_clock_ms'])
    if set(r['identity']) != {'device', 'inode'}:
        raise ValueError('Invalid file identity')
    r['identity'] = FileIdentity(integer(r['identity']['device'], maximum=2**128-1),
                                 integer(r['identity']['inode'], 1, 2**128-1))
    r['_buffer'] = raw(r['_buffer'], MAX_LINE_BYTES-1)
    r['_sample'] = raw(r['_sample'], SAMPLE_BYTES)
    r['_anchor'] = raw(r['_anchor'], ANCHOR_BYTES)
    if not 0 <= r['_line_start'] <= r['_read_offset'] <= r['_observed_size']:
        raise ValueError('Invalid byte positions')
    if len(r['_anchor']) != min(ANCHOR_BYTES, r['_read_offset']):
        raise ValueError('Invalid boundary sample')
    if r['_discard'] is None:
        if r['_read_offset']-r['_line_start'] != len(r['_buffer']) or r['_sample'] or b'\n' in r['_buffer']:
            raise ValueError('Invalid partial line')
    elif r['_discard'] in ('startup', 'oversize'):
        if r['_buffer'] or (r['_discard'] == 'oversize' and r['_read_offset']-r['_line_start'] < MAX_LINE_BYTES):
            raise ValueError('Invalid discard state')
    else:
        raise ValueError('Unknown discard mode')
    reader = LogTailer.__new__(LogTailer)
    reader.__dict__.update(r)

    precise=state['version']==2
    aggregate = (PreciseLogAggregator if precise else LogAggregator)(a['_start_ms'], max_pending_windows=a['_max_pending'])
    integer(a['_next_start_ms'], a['_start_ms'], MAX_TIME_MS-1)
    if a['_next_start_ms'] % 1000:
        raise ValueError('Unaligned aggregate cursor')
    clock(a['_last_clock_ms'])
    boolean(a['_requires_restart'])
    for key in ('_accepted', '_late', '_before_start', '_emitted'):
        integer(a[key])
    if a['_emitted'] != (a['_next_start_ms']-a['_start_ms'])//1000:
        raise ValueError('Invalid aggregate cursor/count')
    if set(a['_parse_errors']) != PARSE_ERROR_CODES:
        raise ValueError('Invalid parse error counters')
    for count in a['_parse_errors'].values():
        integer(count)
    if (a['_late'] or any(a['_parse_errors'].values())) and not a['_requires_restart']:
        raise ValueError('Missing degraded flag')
    if type(a['_buckets']) is not list or len(a['_buckets']) > a['_max_pending']:
        raise ValueError('Too many open windows')
    buckets = {}
    for item in a['_buckets']:
        b = dict(item)
        start = integer(b.pop('start'), a['_next_start_ms'], MAX_TIME_MS-3001)
        if start % 1000 or start in buckets or set(b) != {'count', 'errors_5xx', 'errors_4xx', 'duration_sum', 'first_read', 'last_read'}:
            raise ValueError('Invalid open window')
        count = integer(b['count'], 1, MAX_REQUESTS_PER_WINDOW)
        integer(b['errors_5xx'], maximum=count)
        integer(b['errors_4xx'], maximum=count-b['errors_5xx'])
        integer(b['duration_sum'], maximum=count*MAX_REQUEST_TIME_MS*(1000 if precise else 1))
        integer(b['first_read'], start, start+2999)
        integer(b['last_read'], b['first_read'], start+2999)
        if a['_last_clock_ms'] is None or b['last_read'] > a['_last_clock_ms']:
            raise ValueError('Invalid aggregate read time')
        buckets[start] = _Bucket(**b)
    a['_buckets'] = buckets
    a['_parse_errors'] = dict(a['_parse_errors'])
    aggregate.__dict__.update(a)
    return reader, aggregate


def window_from_dict(data):
    precise=data.get('profile')==PRECISE_PROFILE
    duration_key='duration_sum_us' if precise else 'duration_sum_ms'
    keys = ('start_ms', 'end_ms', 'finalized_at_ms', 'request_count', 'error_5xx_count',
            'error_4xx_count', 'duration_sum_ms', 'first_read_at_ms', 'last_read_at_ms',
            'session_requires_restart')
    keys=tuple(duration_key if key=='duration_sum_ms' else key for key in keys)
    values = {key: data[key] for key in keys}
    start = integer(values['start_ms'], maximum=MAX_TIME_MS-3001)
    if start % 1000 or values['end_ms'] != start+1000:
        raise ValueError('Invalid window bounds')
    integer(values['finalized_at_ms'], start+3000, MAX_TIME_MS-1)
    count = integer(values['request_count'], maximum=MAX_REQUESTS_PER_WINDOW)
    integer(values['error_5xx_count'], maximum=count)
    integer(values['error_4xx_count'], maximum=count-values['error_5xx_count'])
    integer(values[duration_key], maximum=count*MAX_REQUEST_TIME_MS*(1000 if precise else 1))
    boolean(values['session_requires_restart'])
    if count:
        integer(values['first_read_at_ms'], start, start+2999)
        integer(values['last_read_at_ms'], values['first_read_at_ms'], start+2999)
    elif (values['first_read_at_ms'] is not None or values['last_read_at_ms'] is not None or not values['session_requires_restart']):
        raise ValueError('Invalid empty window')
    window = (PreciseLogWindow if precise else LogWindow)(**values)
    if window.as_dict() != data:
        raise ValueError('Window metrics/metadata do not match counters')
    return window
