from __future__ import annotations

import argparse
import datetime as dt
from typing import Any

from normalized_pg import (
    ensure_normalized_schema,
    get_app_timezone,
    get_db_connection,
    rebuild_portfolio_summaries,
)

SOURCE_TABLE = "account_balance_history"
PORTFOLIO_TABLES = (
    "portfolio_balance_history",
    "portfolio_daydiff",
    "portfolio_monthdiff",
)


def fetch_source_summary(cur: Any) -> dict[str, Any]:
    cur.execute(
        f"""
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT account_key) AS account_count,
            MIN(recorded_at) AS min_recorded_at,
            MAX(recorded_at) AS max_recorded_at
        FROM {SOURCE_TABLE}
        """
    )
    row = cur.fetchone()
    return {
        "row_count": int(row[0] or 0),
        "account_count": int(row[1] or 0),
        "min_recorded_at": row[2],
        "max_recorded_at": row[3],
    }


def fetch_portfolio_summary(cur: Any) -> dict[str, int]:
    summary: dict[str, int] = {}
    for table_name in PORTFOLIO_TABLES:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        summary[table_name] = int(cur.fetchone()[0] or 0)
    return summary


def format_timestamp_range(start: dt.datetime | None, end: dt.datetime | None) -> str:
    if start is None or end is None:
        return "no source rows"

    tz = get_app_timezone()
    start_local = start.astimezone(tz)
    end_local = end.astimezone(tz)
    return f"{start_local:%Y-%m-%d %H:%M:%S %Z} ~ {end_local:%Y-%m-%d %H:%M:%S %Z}"


def refresh_portfolios(*, allow_empty_source: bool = False) -> dict[str, Any]:
    connection = get_db_connection()
    try:
        cur = connection.cursor()
        ensure_normalized_schema(cur)

        source_summary = fetch_source_summary(cur)
        if source_summary["row_count"] == 0 and not allow_empty_source:
            raise RuntimeError(
                f"{SOURCE_TABLE} is empty. "
                "Refusing to refresh portfolio tables. "
                "Use --allow-empty-source if an empty rebuild is intentional."
            )

        rebuild_portfolio_summaries(cur)
        portfolio_summary = fetch_portfolio_summary(cur)
        connection.commit()
        return {
            "source": source_summary,
            "portfolio": portfolio_summary,
        }
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def print_refresh_result(result: dict[str, Any]) -> None:
    source = result["source"]
    portfolio = result["portfolio"]

    print("=== Portfolio refresh complete ===")
    print(f"Source table: {SOURCE_TABLE}")
    print(f"Source rows: {source['row_count']}")
    print(f"Accounts: {source['account_count']}")
    print(
        "Recorded range: "
        f"{format_timestamp_range(source['min_recorded_at'], source['max_recorded_at'])}"
    )
    for table_name in PORTFOLIO_TABLES:
        print(f"{table_name}: {portfolio[table_name]} rows")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild portfolio summary tables from account_balance_history "
            "after manual history edits."
        )
    )
    parser.add_argument(
        "--allow-empty-source",
        action="store_true",
        help=(
            "Allow the refresh even when account_balance_history is empty. "
            "This will clear the portfolio summary tables."
        ),
    )
    args = parser.parse_args()

    try:
        result = refresh_portfolios(allow_empty_source=args.allow_empty_source)
    except Exception as exc:
        print(f"[ERROR] portfolio refresh failed: {exc}")
        return 1

    print_refresh_result(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
