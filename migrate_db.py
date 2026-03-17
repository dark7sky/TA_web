from __future__ import annotations

import argparse
import sqlite3
from typing import Any

from normalized_pg import (
    ensure_normalized_schema,
    insert_account_history,
    is_special_account_key,
    parse_legacy_timestamp,
    rebuild_portfolio_summaries,
    upsert_accounts,
    get_db_connection,
)

SQLITE_DB = "stored.db"
LEGACY_SUMMARY_TABLES = {
    "accounts_info",
    "accounts_balance",
    "accounts_diff",
    "accounts_daydiff",
    "accounts_monthdiff",
    "sqlite_sequence",
}


def build_account_tables(sl_cur: sqlite3.Cursor) -> list[str]:
    sl_cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    return [row[0] for row in sl_cur.fetchall() if row[0] not in LEGACY_SUMMARY_TABLES]


def build_metadata_map(sl_cur: sqlite3.Cursor, account_tables: list[str]) -> dict[str, dict[str, str]]:
    metadata = {
        account_key: {
            "company": "",
            "type": "",
            "name": account_key,
            "memo": "",
        }
        for account_key in account_tables
    }

    sl_cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='accounts_info'")
    if sl_cur.fetchone() is None:
        return metadata

    sl_cur.execute("SELECT account_number, company, type, name, memo FROM accounts_info")
    for account_key, company, account_type, name, memo in sl_cur.fetchall():
        metadata[str(account_key)] = {
            "company": "" if company is None else str(company),
            "type": "" if account_type is None else str(account_type),
            "name": str(name) if name not in (None, "") else str(account_key),
            "memo": "" if memo is None else str(memo),
        }
    return metadata


def migrate(execute: bool = False) -> None:
    try:
        sl_conn = sqlite3.connect(SQLITE_DB)
    except sqlite3.Error as exc:
        print(f"Unable to open {SQLITE_DB}: {exc}")
        return

    sl_cur = sl_conn.cursor()
    account_tables = build_account_tables(sl_cur)
    metadata = build_metadata_map(sl_cur, account_tables)

    total_history_rows = 0
    metadata_rows = [
        (
            account_key,
            payload["company"],
            payload["type"],
            payload["name"],
            payload["memo"],
            is_special_account_key(account_key),
            True,
        )
        for account_key, payload in sorted(metadata.items())
    ]

    print(f"[INFO] sqlite account tables: {len(account_tables)}")
    print(f"[INFO] sqlite metadata rows: {len(metadata_rows)}")

    if not execute:
        for account_key in account_tables:
            sl_cur.execute(f'SELECT COUNT(*) FROM "{account_key}"')
            total_history_rows += int(sl_cur.fetchone()[0])
        print(f"[DRY RUN] sqlite history rows: {total_history_rows}")
        sl_conn.close()
        return

    pg_conn = get_db_connection()
    try:
        pg_cur = pg_conn.cursor()
        ensure_normalized_schema(pg_cur)
        upsert_accounts(pg_cur, metadata_rows)

        for account_key in account_tables:
            sl_cur.execute(f'SELECT date, balance FROM "{account_key}" ORDER BY date')
            raw_rows = sl_cur.fetchall()
            history_rows = [
                (
                    account_key,
                    parse_legacy_timestamp(raw_date),
                    int(balance or 0),
                    "sqlite_migration",
                )
                for raw_date, balance in raw_rows
            ]
            total_history_rows += len(history_rows)
            insert_account_history(pg_cur, history_rows)
            print(f"[MIGRATE] {account_key}: {len(history_rows)} rows")

        rebuild_portfolio_summaries(pg_cur)
        pg_conn.commit()
        print(f"[OK] migrated history rows: {total_history_rows}")

        pg_cur.execute("SELECT COUNT(*) FROM accounts")
        print(f"[VERIFY] accounts rows: {int(pg_cur.fetchone()[0])}")
        pg_cur.execute("SELECT COUNT(*) FROM account_balance_history")
        print(f"[VERIFY] account_balance_history rows: {int(pg_cur.fetchone()[0])}")
        pg_cur.execute("SELECT COUNT(*) FROM portfolio_balance_history")
        print(f"[VERIFY] portfolio_balance_history rows: {int(pg_cur.fetchone()[0])}")
    finally:
        pg_conn.close()
        sl_conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate legacy SQLite stored.db into the normalized PostgreSQL schema.")
    parser.add_argument("--execute", action="store_true", help="Run the migration. Without this flag, the script only reports counts.")
    args = parser.parse_args()
    migrate(execute=args.execute)
