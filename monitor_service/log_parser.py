"""Strict parser for ONE explicit Nginx-style text profile, not combined/JSON."""
import re
from .log_schema import AccessRecord, MAX_LINE_BYTES


PARSE_ERROR_CODES = frozenset(('invalid_type', 'line_too_long', 'incomplete_line',
                               'invalid_utf8', 'format_mismatch', 'value_out_of_range'))
_DECIMAL = r'(?:0|[1-9][0-9]{0,10})\.[0-9]{3}'
_LINE = re.compile(r'v1 ts=(?P<timestamp>' + _DECIMAL +
                   r') status=(?P<status>[0-9]{3}) rt=(?P<duration>' + _DECIMAL + r')', re.ASCII)


class LogParseError(ValueError):
    def __init__(self, code):
        if code not in PARSE_ERROR_CODES:
            raise ValueError('Unknown parse error code')
        self.code = code
        super().__init__(code)


def _milliseconds(decimal):
    seconds, fraction = decimal.split('.')
    return int(seconds) * 1000 + int(fraction)


def parse_line(line):
    """Parse a complete bytes line including LF (optionally CRLF).

    A future tailer must wait for LF before calling. No strip(), guessed units,
    float timestamps, forgiving decoding or replacement of missing values.
    """
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
    if match is None:
        raise LogParseError('format_mismatch')
    try:
        return AccessRecord(timestamp_ms=_milliseconds(match['timestamp']),
                            status=int(match['status']),
                            request_time_ms=_milliseconds(match['duration']))
    except ValueError as exc:
        raise LogParseError('value_out_of_range') from exc
