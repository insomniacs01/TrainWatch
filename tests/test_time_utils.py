import unittest

from app.time_utils import coerce_utc_timestamp, format_utc, parse_utc_timestamp


class TimeUtilsTests(unittest.TestCase):
    def test_parse_utc_timestamp_accepts_unix_seconds(self) -> None:
        parsed = parse_utc_timestamp("1710504000")
        self.assertEqual(format_utc(parsed), "2024-03-15T12:00:00Z")

    def test_parse_utc_timestamp_accepts_iso8601(self) -> None:
        parsed = parse_utc_timestamp("2026-03-15T08:30:00Z")
        self.assertEqual(format_utc(parsed), "2026-03-15T08:30:00Z")

    def test_coerce_utc_timestamp_uses_default_delta(self) -> None:
        value = coerce_utc_timestamp(None, default_delta_hours=6)
        reparsed = parse_utc_timestamp(value)
        self.assertEqual(format_utc(reparsed), value)
