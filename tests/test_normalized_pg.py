import datetime as dt
import unittest
from unittest.mock import MagicMock, patch

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


class PortfolioSummaryUpdateTests(unittest.TestCase):
    def test_rebuilds_when_summary_table_is_empty(self) -> None:
        cur = MagicMock()
        cur.fetchone.return_value = (False,)
        recorded_at = dt.datetime(2026, 7, 6, 12, 0, tzinfo=dt.timezone.utc)

        with patch.object(normalized_pg, "rebuild_portfolio_summaries") as rebuild:
            result = normalized_pg.update_portfolio_summaries(cur, recorded_at)

        self.assertEqual(result, "rebuilt")
        rebuild.assert_called_once_with(cur)

    def test_incremental_update_does_not_rebuild_history(self) -> None:
        tz = dt.timezone.utc
        recorded_at = dt.datetime(2026, 7, 6, 12, 0, tzinfo=tz)
        cur = MagicMock()
        cur.fetchone.return_value = (True,)
        cur.fetchall.return_value = [
            (dt.datetime(2026, 7, 5, 12, 0, tzinfo=tz), 100),
            (recorded_at, 130),
        ]

        with (
            patch.object(normalized_pg, "fetch_current_portfolio_total", return_value=130),
            patch.object(normalized_pg, "rebuild_portfolio_summaries") as rebuild,
            patch.object(normalized_pg.psycopg2.extras, "execute_values"),
            patch.object(normalized_pg, "now_local", return_value=recorded_at),
        ):
            result = normalized_pg.update_portfolio_summaries(cur, recorded_at)

        self.assertEqual(result, "updated")
        rebuild.assert_not_called()


if __name__ == '__main__':
    unittest.main()
