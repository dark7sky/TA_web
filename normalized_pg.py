from __future__ import annotations

import datetime as dt
import os
import re
from dataclasses import dataclass
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

APP_TIMEZONE_ENV = "APP_TIMEZONE"
DEFAULT_TIMEZONE = "Asia/Seoul"
KNOWN_SPECIAL_KEYS = {
    "accounts_cards",
    "toss",
    "debt",
    "insurance",
    "lab_private",
    "bithumb",
}


@dataclass(frozen=True)
class AccountRow:
    account_key: str
    company: str
    account_type: str
    name: str
    memo: str
    is_special: bool
    is_active: bool
    latest_balance: int
    latest_recorded_at: dt.datetime | None


def required_env(key: str) -> str:
    value = os.getenv(key)
    if value in (None, ""):
        raise RuntimeError(f"Required env var missing: {key}")
    return value


def get_app_timezone() -> dt.tzinfo:
    try:
        return ZoneInfo(os.getenv(APP_TIMEZONE_ENV, DEFAULT_TIMEZONE))
    except ZoneInfoNotFoundError:
        return dt.timezone.utc


def is_numeric_account_key(account_key: str) -> bool:
    return re.fullmatch(r"\d+", account_key or "") is not None


def is_special_account_key(account_key: str) -> bool:
    return account_key in KNOWN_SPECIAL_KEYS or not is_numeric_account_key(account_key)


def now_local() -> dt.datetime:
    return dt.datetime.now(get_app_timezone())


def append_as_of_balance_row(rows: list[tuple[dt.datetime, int]]) -> list[tuple[dt.datetime, int]]:
    if not rows:
        return rows

    as_of = now_local().replace(microsecond=0)
    latest_recorded_at, latest_balance = rows[-1]
    if latest_recorded_at >= as_of:
        return rows
    return [*rows, (as_of, latest_balance)]


def get_db_connection() -> Any:
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return psycopg2.connect(database_url)
    return psycopg2.connect(
        host=required_env("DB_HOST"),
        database=required_env("DB_NAME"),
        user=required_env("DB_USER"),
        password=required_env("DB_PASSWORD"),
        port=os.getenv("DB_PORT", "5432"),
        sslmode=os.getenv("DB_SSLMODE", "disable"),
    )


def parse_legacy_timestamp(raw: Any) -> dt.datetime:
    tzinfo = get_app_timezone()

    if isinstance(raw, dt.datetime):
        return raw if raw.tzinfo is not None else raw.replace(tzinfo=tzinfo)

    if isinstance(raw, dt.date):
        return dt.datetime.combine(raw, dt.time.min, tzinfo=tzinfo)

    text = str(raw).strip()
    if not text:
        raise ValueError("Empty timestamp value")

    iso_text = text.replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(iso_text)
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=tzinfo)
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(text, fmt).replace(tzinfo=tzinfo)
        except ValueError:
            continue

    raise ValueError(f"Unsupported timestamp format: {raw!r}")


def ensure_normalized_schema(cur: Any) -> None:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS accounts (
            account_key TEXT PRIMARY KEY,
            company TEXT,
            type TEXT,
            name TEXT,
            memo TEXT NOT NULL DEFAULT '',
            is_special BOOLEAN NOT NULL DEFAULT FALSE,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS account_balance_history (
            account_key TEXT NOT NULL REFERENCES accounts(account_key) ON DELETE CASCADE,
            recorded_at TIMESTAMPTZ NOT NULL,
            balance BIGINT NOT NULL,
            source TEXT NOT NULL,
            PRIMARY KEY (account_key, recorded_at)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS portfolio_balance_history (
            recorded_at TIMESTAMPTZ PRIMARY KEY,
            balance BIGINT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS portfolio_daydiff (
            balance_date DATE PRIMARY KEY,
            balance BIGINT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS portfolio_monthdiff (
            balance_date DATE PRIMARY KEY,
            balance BIGINT NOT NULL
        )
        """,
        'CREATE INDEX IF NOT EXISTS "idx_account_balance_history_account_recorded_at" ON "account_balance_history" ("account_key", "recorded_at" DESC)',
        'CREATE INDEX IF NOT EXISTS "idx_account_balance_history_recorded_at" ON "account_balance_history" ("recorded_at" DESC)',
        'CREATE INDEX IF NOT EXISTS "idx_portfolio_balance_history_recorded_at" ON "portfolio_balance_history" ("recorded_at" DESC)',
        'CREATE INDEX IF NOT EXISTS "idx_portfolio_daydiff_balance_date" ON "portfolio_daydiff" ("balance_date" DESC)',
        'CREATE INDEX IF NOT EXISTS "idx_portfolio_monthdiff_balance_date" ON "portfolio_monthdiff" ("balance_date" DESC)',
    ]
    for statement in statements:
        cur.execute(statement)


def chunked(rows: Sequence[tuple[Any, ...]], size: int = 2000) -> list[Sequence[tuple[Any, ...]]]:
    return [rows[index : index + size] for index in range(0, len(rows), size)]


def delete_redundant_account_history(cur: Any) -> int:
    cur.execute(
        """
        WITH redundant_rows AS (
            SELECT
                account_key,
                recorded_at
            FROM (
                SELECT
                    account_key,
                    recorded_at,
                    balance,
                    LAG(balance) OVER (
                        PARTITION BY account_key
                        ORDER BY recorded_at
                    ) AS previous_balance
                FROM account_balance_history
            ) history
            WHERE balance = previous_balance
        )
        DELETE FROM account_balance_history target
        USING redundant_rows
        WHERE target.account_key = redundant_rows.account_key
          AND target.recorded_at = redundant_rows.recorded_at
        """
    )
    return cur.rowcount


def compute_portfolio_rows(cur: Any) -> list[tuple[dt.datetime, int]]:
    cur.execute(
        """
        SELECT account_key, recorded_at, balance
        FROM account_balance_history
        ORDER BY recorded_at, account_key
        """
    )
    latest_by_account: dict[str, int] = {}
    total = 0
    current_ts: dt.datetime | None = None
    portfolio_rows: list[tuple[dt.datetime, int]] = []

    for account_key, recorded_at, balance in cur.fetchall():
        if current_ts is not None and recorded_at != current_ts:
            portfolio_rows.append((current_ts, total))
        balance_int = int(balance)
        previous = latest_by_account.get(account_key, 0)
        total += balance_int - previous
        latest_by_account[account_key] = balance_int
        current_ts = recorded_at

    if current_ts is not None:
        portfolio_rows.append((current_ts, total))
    return portfolio_rows


def extend_portfolio_rows_to_now(portfolio_rows: list[tuple[dt.datetime, int]]) -> list[tuple[dt.datetime, int]]:
    return append_as_of_balance_row(portfolio_rows)


def build_daydiff_rows(portfolio_rows: Iterable[tuple[dt.datetime, int]]) -> list[tuple[dt.date, int]]:
    day_totals: dict[dt.date, int] = {}
    for recorded_at, balance in portfolio_rows:
        local_date = recorded_at.astimezone(get_app_timezone()).date()
        day_totals[local_date] = int(balance)

    ordered = sorted(day_totals.items())
    result: list[tuple[dt.date, int]] = []
    previous_total: int | None = None
    for balance_date, total in ordered:
        if previous_total is None:
            previous_total = total
            continue
        result.append((balance_date, total - previous_total))
        previous_total = total
    return result


def build_monthdiff_rows(portfolio_rows: Iterable[tuple[dt.datetime, int]]) -> list[tuple[dt.date, int]]:
    month_totals: dict[tuple[int, int], int] = {}
    month_labels: dict[tuple[int, int], dt.date] = {}
    for recorded_at, balance in portfolio_rows:
        local_stamp = recorded_at.astimezone(get_app_timezone())
        month_key = (local_stamp.year, local_stamp.month)
        month_totals[month_key] = int(balance)
        month_labels[month_key] = local_stamp.date()

    ordered_keys = sorted(month_totals)
    result: list[tuple[dt.date, int]] = []
    previous_total: int | None = None
    for month_key in ordered_keys:
        total = month_totals[month_key]
        if previous_total is None:
            previous_total = total
            continue
        result.append((month_labels[month_key], total - previous_total))
        previous_total = total
    return result


def rebuild_portfolio_summaries(cur: Any) -> None:
    portfolio_rows = compute_portfolio_rows(cur)
    portfolio_rows_for_periods = extend_portfolio_rows_to_now(portfolio_rows)
    daydiff_rows = build_daydiff_rows(portfolio_rows_for_periods)
    monthdiff_rows = build_monthdiff_rows(portfolio_rows_for_periods)

    cur.execute("TRUNCATE TABLE portfolio_balance_history, portfolio_daydiff, portfolio_monthdiff")

    for chunk in chunked(portfolio_rows):
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO portfolio_balance_history (recorded_at, balance)
            VALUES %s
            ON CONFLICT (recorded_at) DO UPDATE SET balance = EXCLUDED.balance
            """,
            chunk,
        )

    for chunk in chunked(daydiff_rows):
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO portfolio_daydiff (balance_date, balance)
            VALUES %s
            ON CONFLICT (balance_date) DO UPDATE SET balance = EXCLUDED.balance
            """,
            chunk,
        )

    for chunk in chunked(monthdiff_rows):
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO portfolio_monthdiff (balance_date, balance)
            VALUES %s
            ON CONFLICT (balance_date) DO UPDATE SET balance = EXCLUDED.balance
            """,
            chunk,
        )


def upsert_accounts(cur: Any, rows: Sequence[tuple[Any, ...]]) -> None:
    if not rows:
        return
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO accounts (
            account_key, company, type, name, memo, is_special, is_active
        ) VALUES %s
        ON CONFLICT (account_key) DO UPDATE SET
            company = CASE WHEN EXCLUDED.company <> '' THEN EXCLUDED.company ELSE accounts.company END,
            type = CASE WHEN EXCLUDED.type <> '' THEN EXCLUDED.type ELSE accounts.type END,
            name = CASE WHEN EXCLUDED.name <> '' THEN EXCLUDED.name ELSE accounts.name END,
            memo = CASE WHEN EXCLUDED.memo <> '' THEN EXCLUDED.memo ELSE accounts.memo END,
            is_special = accounts.is_special OR EXCLUDED.is_special,
            is_active = EXCLUDED.is_active,
            updated_at = now()
        """,
        rows,
    )


def insert_account_history(cur: Any, rows: Sequence[tuple[Any, ...]]) -> None:
    if not rows:
        return
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO account_balance_history (account_key, recorded_at, balance, source)
        VALUES %s
        ON CONFLICT (account_key, recorded_at) DO UPDATE SET
            balance = EXCLUDED.balance,
            source = EXCLUDED.source
        """,
        rows,
    )


def fetch_accounts_with_latest(cur: Any, include_inactive: bool = True) -> list[AccountRow]:
    sql = """
        SELECT
            a.account_key,
            COALESCE(a.company, ''),
            COALESCE(a.type, ''),
            COALESCE(a.name, a.account_key),
            COALESCE(a.memo, ''),
            a.is_special,
            a.is_active,
            COALESCE(latest.balance, 0) AS latest_balance,
            latest.recorded_at
        FROM accounts a
        LEFT JOIN LATERAL (
            SELECT balance, recorded_at
            FROM account_balance_history h
            WHERE h.account_key = a.account_key
            ORDER BY h.recorded_at DESC
            LIMIT 1
        ) latest ON TRUE
    """
    params: tuple[Any, ...] = tuple()
    if not include_inactive:
        sql += " WHERE a.is_active = TRUE"
    sql += " ORDER BY a.account_key"
    cur.execute(sql, params)
    return [
        AccountRow(
            account_key=row[0],
            company=row[1],
            account_type=row[2],
            name=row[3],
            memo=row[4],
            is_special=bool(row[5]),
            is_active=bool(row[6]),
            latest_balance=int(row[7] or 0),
            latest_recorded_at=row[8],
        )
        for row in cur.fetchall()
    ]


def fetch_account_history(cur: Any, account_key: str, limit: int | None = None) -> list[tuple[dt.datetime, int, str]]:
    sql = """
        SELECT recorded_at, balance, source
        FROM account_balance_history
        WHERE account_key = %s
        ORDER BY recorded_at DESC
    """
    params: list[Any] = [account_key]
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
    cur.execute(sql, tuple(params))
    return [(row[0], int(row[1]), str(row[2])) for row in cur.fetchall()]


def fetch_portfolio_history(cur: Any, limit: int | None = None) -> list[tuple[dt.datetime, int]]:
    sql = "SELECT recorded_at, balance FROM portfolio_balance_history ORDER BY recorded_at DESC"
    params: tuple[Any, ...] = tuple()
    if limit is not None:
        sql += " LIMIT %s"
        params = (limit,)
    cur.execute(sql, params)
    descending_rows = [(row[0], int(row[1])) for row in cur.fetchall()]
    ascending_rows = list(reversed(descending_rows))
    return list(reversed(append_as_of_balance_row(ascending_rows)))


def fetch_summary_rows(cur: Any, table_name: str, limit: int = 200) -> list[tuple[Any, int]]:
    if table_name == "accounts_balance":
        cur.execute(
            """
            SELECT recorded_at, balance
            FROM portfolio_balance_history
            ORDER BY recorded_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        descending_rows = [(row[0], int(row[1])) for row in cur.fetchall()]
        ascending_rows = list(reversed(descending_rows))
        return list(reversed(append_as_of_balance_row(ascending_rows)))

    if table_name == "accounts_daydiff":
        cur.execute(
            """
            SELECT balance_date, balance
            FROM portfolio_daydiff
            ORDER BY balance_date DESC
            LIMIT %s
            """,
            (limit,),
        )
        return [(row[0], int(row[1])) for row in cur.fetchall()]

    if table_name == "accounts_monthdiff":
        cur.execute(
            """
            SELECT balance_date, balance
            FROM portfolio_monthdiff
            ORDER BY balance_date DESC
            LIMIT %s
            """,
            (limit,),
        )
        return [(row[0], int(row[1])) for row in cur.fetchall()]

    if table_name == "accounts_diff":
        cur.execute(
            """
            SELECT recorded_at, balance
            FROM portfolio_balance_history
            ORDER BY recorded_at DESC
            LIMIT %s
            """,
            (limit + 1,),
        )
        descending_rows = [(row[0], int(row[1])) for row in cur.fetchall()]
        rows = append_as_of_balance_row(list(reversed(descending_rows)))
        diffs: list[tuple[dt.datetime, int]] = []
        previous_balance: int | None = None
        for recorded_at, balance in rows:
            if previous_balance is None:
                previous_balance = balance
                continue
            diffs.append((recorded_at, balance - previous_balance))
            previous_balance = balance
        return list(reversed(diffs[-limit:]))

    raise ValueError(f"Unsupported summary table alias: {table_name}")
