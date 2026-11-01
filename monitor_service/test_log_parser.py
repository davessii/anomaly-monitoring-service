"""Strict log-format contract tests; no live file tailing or model calls."""
from dataclasses import FrozenInstanceError
import unittest
from .log_parser import LogParseError, parse_line
from .log_schema import (AccessRecord, FEATURES, MAX_LINE_BYTES, MAX_TIME_MS,
                         MAX_REQUEST_TIME_MS)


class LogParserTests(unittest.TestCase):
    def fails(self, raw, code):
        with self.assertRaises(LogParseError) as caught:
            parse_line(raw)
        self.assertEqual(caught.exception.code, code)

    def test_valid_request_uses_exact_integer_milliseconds(self):
        row = parse_line(b'v1 ts=1792490400.123 status=200 rt=0.042\n')
        self.assertEqual(row, AccessRecord(1792490400123, 200, 42))

    def test_crlf_and_zero_duration(self):
        self.assertEqual(parse_line(b'v1 ts=1.001 status=204 rt=0.000\r\n'),
                         AccessRecord(1001, 204, 0))

    def test_one_millisecond_and_second_boundary_are_not_floats(self):
        a = parse_line(b'v1 ts=1792490400.999 status=200 rt=1.001\n')
        b = parse_line(b'v1 ts=1792490401.000 status=200 rt=0.001\n')
        self.assertEqual(b.timestamp_ms-a.timestamp_ms, 1)
        self.assertEqual((a.request_time_ms, b.request_time_ms), (1001, 1))

    def test_valid_status_boundaries(self):
        for status in (100, 199, 200, 299, 300, 399, 400, 499, 500, 599):
            with self.subTest(status=status):
                self.assertEqual(parse_line(f'v1 ts=1.000 status={status} rt=0.001\n'.encode()).status, status)

    def test_invalid_status_is_not_success(self):
        for status in ('000', '099', '600', '999'):
            with self.subTest(status=status):
                self.fails(f'v1 ts=1.000 status={status} rt=0.001\n'.encode(), 'value_out_of_range')

    def test_missing_newline_is_incomplete_not_a_record(self):
        self.fails(b'v1 ts=1.000 status=200 rt=0.010', 'incomplete_line')
        self.fails(b'v1 ts=1.000 status=200 rt=0.010\r', 'incomplete_line')

    def test_only_one_line_and_no_surrounding_whitespace(self):
        valid = b'v1 ts=1.000 status=200 rt=0.010'
        for raw in (b' '+valid+b'\n', valid+b' \n', valid+b'\n\n', valid+b'\n'+valid+b'\n',
                    valid+b'\r\r\n', b'\n'):
            with self.subTest(raw=raw):
                self.fails(raw, 'format_mismatch')

    def test_profile_order_names_and_extra_fields_are_strict(self):
        for raw in (b'v2 ts=1.000 status=200 rt=0.010\n',
                    b'v1 status=200 ts=1.000 rt=0.010\n',
                    b'v1 ts=1.000 status=200 request_time=0.010\n',
                    b'v1 ts=1.000 status=200 rt=0.010 extra=1\n',
                    b'127.0.0.1 - - [20/Oct/2026:12:00:00 +0300] "GET / HTTP/1.1" 200 20\n',
                    b'{"ts":1,"status":200,"rt":0.01}\n'):
            with self.subTest(raw=raw):
                self.fails(raw, 'format_mismatch')

    def test_no_guessed_precision_nonfinite_negative_or_missing_values(self):
        for value in ('0.01', '0.0100', '1e-3', 'nan', 'NaN', 'inf', '-0.001', '+0.010', '-', '00.010'):
            with self.subTest(value=value):
                self.fails(f'v1 ts=1.000 status=200 rt={value}\n'.encode(), 'format_mismatch')

    def test_timestamp_precision_and_ascii_digits_only(self):
        for value in ('1', '1.00', '01.000', '-1.000', '１.000', '1.０００'):
            with self.subTest(value=value):
                self.fails(f'v1 ts={value} status=200 rt=0.010\n'.encode(), 'format_mismatch')

    def test_utf8_is_strict_and_bom_is_not_ignored(self):
        self.fails(b'\xff\n', 'invalid_utf8')
        self.fails(b'\xe2\x82\n', 'invalid_utf8')
        self.fails(b'\xef\xbb\xbfv1 ts=1.000 status=200 rt=0.010\n', 'format_mismatch')

    def test_byte_limit_includes_line_ending(self):
        self.fails(b'x'*(MAX_LINE_BYTES-1)+b'\n', 'format_mismatch')
        self.fails(b'x'*MAX_LINE_BYTES+b'\n', 'line_too_long')
        self.fails(b'x'*(MAX_LINE_BYTES+1), 'line_too_long')

    def test_only_bytes_not_implicit_string_conversion(self):
        for raw in ('v1 ts=1.000 status=200 rt=0.010\n', bytearray(b'x\n'), None, 1):
            with self.subTest(raw=raw):
                self.fails(raw, 'invalid_type')

    def test_numeric_ranges_are_bounded(self):
        self.fails(b'v1 ts=0.000 status=200 rt=0.010\n', 'value_out_of_range')
        self.fails(b'v1 ts=99999999999.999 status=200 rt=0.010\n', 'value_out_of_range')
        self.fails(b'v1 ts=1.000 status=200 rt=1000000000.001\n', 'value_out_of_range')
        self.assertEqual(parse_line(b'v1 ts=1.000 status=200 rt=1000000000.000\n').request_time_ms,
                         MAX_REQUEST_TIME_MS)

    def test_direct_record_constructor_cannot_bypass_validation(self):
        for args in ((True, 200, 1), (1, True, 1), (1, 200, False), (1.0, 200, 1),
                     (0, 200, 1), (MAX_TIME_MS, 200, 1), (1, 600, 1), (1, 200, -1)):
            with self.subTest(args=args):
                with self.assertRaises(ValueError):
                    AccessRecord(*args)

    def test_records_are_immutable_and_feature_order_is_fixed(self):
        row = AccessRecord(1000, 200, 1)
        with self.assertRaises(FrozenInstanceError):
            row.status = 500
        self.assertEqual(FEATURES, ('completed_rps', 'mean_request_ms', 'server_error_fraction'))


if __name__ == '__main__':
    unittest.main()
