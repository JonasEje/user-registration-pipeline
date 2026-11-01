import unittest
from datetime import datetime, timezone

from spark.export_scheduler import (
    next_export_time,
    seconds_until_next_export,
)


class ExportScheduleTests(unittest.TestCase):
    def test_next_export_is_the_coming_midnight(self) -> None:
        now = datetime(2026, 11, 1, 12, 30, tzinfo=timezone.utc)
        self.assertEqual(
            next_export_time(now),
            datetime(2026, 11, 2, 0, 0, tzinfo=timezone.utc),
        )

    def test_midnight_rolls_to_the_next_day(self) -> None:
        now = datetime(2026, 11, 1, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(
            next_export_time(now),
            datetime(2026, 11, 2, 0, 0, tzinfo=timezone.utc),
        )

    def test_seconds_until_next_export_counts_down_to_midnight(self) -> None:
        now = datetime(2026, 11, 1, 23, 0, tzinfo=timezone.utc)
        self.assertEqual(seconds_until_next_export(now), 3600.0)

    def test_seconds_until_next_export_is_never_negative(self) -> None:
        now = datetime(2026, 11, 1, 23, 59, 59, tzinfo=timezone.utc)
        self.assertGreater(seconds_until_next_export(now), 0.0)


if __name__ == "__main__":
    unittest.main()
