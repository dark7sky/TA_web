import datetime as dt
import unittest
from unittest.mock import patch

import normalized_pg


class AppendAsOfBalanceRowTests(unittest.TestCase):
    def test_appends_current_balance_when_latest_point_is_in_the_past(self) -> None:
        tz = dt.timezone.utc
        rows = [(dt.datetime(2026, 3, 20, 10, 0, tzinfo=tz), 100)]
        with patch.object(normalized_pg, 'now_local', return_value=dt.datetime(2026, 3, 20, 11, 30, 45, tzinfo=tz)):
            result = normalized_pg.append_as_of_balance_row(rows)
        self.assertEqual(
            result,
            [
                (dt.datetime(2026, 3, 20, 10, 0, tzinfo=tz), 100),
                (dt.datetime(2026, 3, 20, 11, 30, 45, tzinfo=tz).replace(microsecond=0), 100),
            ],
        )

    def test_skips_append_when_latest_point_is_already_current(self) -> None:
        tz = dt.timezone.utc
        rows = [(dt.datetime(2026, 3, 20, 12, 0, tzinfo=tz), 200)]
        with patch.object(normalized_pg, 'now_local', return_value=dt.datetime(2026, 3, 20, 11, 59, 59, tzinfo=tz)):
            result = normalized_pg.append_as_of_balance_row(rows)
        self.assertEqual(result, rows)


if __name__ == '__main__':
    unittest.main()
