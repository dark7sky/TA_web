from __future__ import annotations

import datetime as dt
import os
import pickle
import shutil
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import cards
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from tqdm import tqdm

import logger

load_dotenv()

logs = logger.logger(Path(__file__).stem)


def log(message: str) -> None:
    logs.msg(message)


FILE_LAST_UPDATE = Path(".last_update.pickle")
FILE_LAST_UPDATE_BK = Path(".last_update_bk.pickle")
FILE_KB_PICKLE = Path("KB.pickle")
FILE_CARD_PICKLE = Path("cards.pickle")
FILE_CARD_EXCEL = Path("카드통합.xlsx")
SYNCED_CARD_EXCEL = Path(r"Z:/ResilioSync/Total_Account_Tracker_210308/카드통합.xlsx")

MANUAL_INPUTS_TABLE = "manual_inputs"
SYSTEM_SETTINGS_TABLE = "system_settings"
APP_TIMEZONE_ENV = "APP_TIMEZONE"
EXCEPTION_BANKS_ENV = "EXCEPTION_BANKS"
EXCEPTION_ACCOUNTS_ENV = "EXCEPTION_ACCOUNTS"
TA_WEB_LAST_CRAWLED_AT_KEY = "ta_web_last_crawled_at"
USE_TQDM = True

KNOWN_SPECIAL_KEYS = {
    "accounts_cards",
    "debt",
    "insurance",
    "lab_private",
    "외화",
    "지역화폐",
    "장기수선충당금",
}

MANUAL_KEY_ALIASES = {
    "랩비": "lab_private",
    "대출": "debt",
    "보험": "insurance",
}


@dataclass
class AccountSnapshot:
    account_key: str
    balance: int
    company: str = ""
    account_type: str = ""
    name: str = ""
    memo: str = ""
    is_special: bool = False
    source: str = "kb"


@dataclass(frozen=True)
class ExclusionRules:
    banks: frozenset[str]
    accounts: frozenset[str]

    def excludes(self, account_key: str, company: str) -> bool:
        return account_key in self.accounts or company in self.banks


@dataclass
class RunContext:
    recorded_at: dt.datetime
    last_update: dict[str, float]
    exclusions: ExclusionRules


Accounts = dict[str, AccountSnapshot]


def progress_iter(iterable: Any, desc: str = "") -> Any:
    return tqdm(iterable, desc=desc) if USE_TQDM else iterable


def required_env(key: str) -> str:
    value = os.getenv(key)
    if value in (None, ""):
        raise RuntimeError(f"Required env var missing: {key}")
    return value


def get_app_timezone() -> ZoneInfo | dt.timezone:
    try:
        return ZoneInfo(os.getenv(APP_TIMEZONE_ENV, "Asia/Seoul"))
    except ZoneInfoNotFoundError:
        return dt.timezone.utc


def now_local() -> dt.datetime:
    return dt.datetime.now(get_app_timezone())


def parse_env_csv(key: str) -> list[str]:
    raw_value = os.getenv(key, "")
    if raw_value in (None, ""):
        return []
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def load_exclusion_rules() -> ExclusionRules:
    return ExclusionRules(
        banks=frozenset(parse_env_csv(EXCEPTION_BANKS_ENV)),
        accounts=frozenset(parse_env_csv(EXCEPTION_ACCOUNTS_ENV)),
    )


def default_last_update_state() -> dict[str, float]:
    return {"KB": 0.0}


def load_last_update_state() -> dict[str, float]:
    try:
        with FILE_LAST_UPDATE.open("rb") as file_obj:
            loaded = pickle.load(file_obj)
        if isinstance(loaded, dict):
            return loaded
        raise TypeError(f"{FILE_LAST_UPDATE} must contain a dict")
    except FileNotFoundError:
        log(f"{FILE_LAST_UPDATE} not found; using defaults.")
        return default_last_update_state()
    except pickle.UnpicklingError:
        log(f"{FILE_LAST_UPDATE} is corrupt; loading backup.")
        with FILE_LAST_UPDATE_BK.open("rb") as file_obj:
            loaded = pickle.load(file_obj)
        if isinstance(loaded, dict):
            return loaded
        raise TypeError(f"{FILE_LAST_UPDATE_BK} must contain a dict")


def save_last_update_state(last_update: dict[str, float]) -> bool:
    try:
        with FILE_LAST_UPDATE.open("wb") as file_obj:
            pickle.dump(last_update, file_obj)
        return True
    except Exception as exc:
        log(f"Error saving {FILE_LAST_UPDATE}: {exc}")
        return False


def parse_int_value(raw_value: Any) -> int | None:
    if raw_value is None:
        return None
    if isinstance(raw_value, bool):
        return int(raw_value)
    if isinstance(raw_value, int):
        return raw_value
    if isinstance(raw_value, Decimal):
        return int(raw_value)
    if isinstance(raw_value, float):
        return int(raw_value)

    text_value = str(raw_value).strip().replace(",", "")
    if text_value == "":
        return None
    try:
        return int(text_value)
    except ValueError:
        try:
            return int(float(text_value))
        except ValueError:
            return None


def get_db_connection() -> Any:
    return psycopg2.connect(
        host=required_env("DB_HOST"),
        database=required_env("DB_NAME"),
        user=required_env("DB_USER"),
        password=required_env("DB_PASSWORD"),
        port=os.getenv("DB_PORT", "5432"),
        sslmode="disable",
    )


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
        f"""
        CREATE TABLE IF NOT EXISTS {SYSTEM_SETTINGS_TABLE} (
            id SERIAL PRIMARY KEY,
            setting_key VARCHAR(100) UNIQUE NOT NULL,
            setting_value JSON NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
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


def upsert_system_setting(cur: Any, setting_key: str, setting_value: Any) -> None:
    cur.execute(
        f"""
        INSERT INTO {SYSTEM_SETTINGS_TABLE} (setting_key, setting_value)
        VALUES (%s, %s)
        ON CONFLICT (setting_key) DO UPDATE SET
            setting_value = EXCLUDED.setting_value,
            updated_at = now()
        """,
        (setting_key, psycopg2.extras.Json(setting_value)),
    )


def write_crawl_heartbeat(cur: Any, recorded_at: dt.datetime, account_count: int) -> None:
    upsert_system_setting(
        cur,
        TA_WEB_LAST_CRAWLED_AT_KEY,
        {
            "crawled_at": recorded_at.isoformat(),
            "account_count": account_count,
            "source": "KB.py",
        },
    )


def merge_snapshot(accounts: Accounts, snapshot: AccountSnapshot, additive: bool = False) -> None:
    existing = accounts.get(snapshot.account_key)
    if existing is None:
        if not snapshot.name:
            snapshot.name = snapshot.account_key
        accounts[snapshot.account_key] = snapshot
        return

    if snapshot.company:
        existing.company = snapshot.company
    if snapshot.account_type:
        existing.account_type = snapshot.account_type
    if snapshot.name:
        existing.name = snapshot.name
    if snapshot.memo:
        existing.memo = snapshot.memo
    existing.is_special = existing.is_special or snapshot.is_special
    existing.balance = existing.balance + snapshot.balance if additive else snapshot.balance
    if snapshot.source:
        existing.source = snapshot.source


def normalize_manual_key(raw_key: str) -> tuple[str, str | None]:
    if "@" in raw_key:
        head, *tail = raw_key.split("@")
        return MANUAL_KEY_ALIASES.get(head, head), "".join(tail) or None
    return MANUAL_KEY_ALIASES.get(raw_key, raw_key), None


def build_special_snapshot(account_key: str, balance: int, source: str, memo: str = "") -> AccountSnapshot:
    return AccountSnapshot(
        account_key=account_key,
        balance=balance,
        company=account_key,
        account_type="Special",
        name=account_key,
        memo=memo,
        is_special=True,
        source=source,
    )


def parse_openbank_row(row: Any, exclusions: ExclusionRules) -> AccountSnapshot | None:
    if not isinstance(row, (list, tuple)) or len(row) < 5:
        log(f"Skip malformed KB row: {row!r}")
        return None

    company = str(row[0]).strip()
    account_type = str(row[1]).strip()
    account_key = str(row[2]).strip()
    name = str(row[3]).strip() or account_key
    balance = parse_int_value(row[4])

    if not account_key or balance is None:
        log(f"Skip KB row with invalid account or balance: {row!r}")
        return None
    if exclusions.excludes(account_key, company):
        return None

    return AccountSnapshot(
        account_key=account_key,
        balance=balance,
        company=company,
        account_type=account_type,
        name=name,
        source="kb_openbank",
    )


def load_openbank_accounts(exclusions: ExclusionRules) -> Accounts:
    with FILE_KB_PICKLE.open("rb") as file_obj:
        loaded = pickle.load(file_obj)

    if not isinstance(loaded, list):
        raise TypeError(f"{FILE_KB_PICKLE} must contain a list of rows")

    accounts: Accounts = {}
    for row in loaded:
        snapshot = parse_openbank_row(row, exclusions)
        if snapshot is not None:
            merge_snapshot(accounts, snapshot)
    return accounts


def apply_manual_inputs_from_db(cur: Any, accounts: Accounts) -> None:
    try:
        cur.execute(
            f'SELECT key_name, value FROM "{MANUAL_INPUTS_TABLE}" ORDER BY key_name'
        )
    except (psycopg2.OperationalError, psycopg2.errors.UndefinedTable) as exc:
        log(f"{MANUAL_INPUTS_TABLE} is not ready: {exc}")
        return

    for raw_key, raw_value in cur.fetchall():
        if raw_key is None:
            continue
        key_name = str(raw_key).strip()
        if not key_name:
            continue
        value = parse_int_value(raw_value)
        if value is None:
            log(f"Skip manual input with non-numeric value: {key_name}={raw_value!r}")
            continue

        account_key, label = normalize_manual_key(key_name)
        snapshot = build_special_snapshot(
            account_key=account_key,
            balance=value,
            source="manual_inputs",
            memo=label or "",
        )
        merge_snapshot(accounts, snapshot, additive="@" in key_name)


def should_refresh_cards(last_update: dict[str, float]) -> bool:
    if not FILE_CARD_EXCEL.is_file():
        return False
    if "cards" not in last_update:
        return True
    if not FILE_CARD_PICKLE.is_file():
        return True
    card_pickle_date = dt.date.fromtimestamp(FILE_CARD_PICKLE.stat().st_mtime)
    if card_pickle_date != now_local().date():
        return True
    return FILE_CARD_EXCEL.stat().st_mtime != last_update["cards"]


def fetch_latest_card_balance(cur: Any) -> int:
    try:
        cur.execute(
            """
            SELECT balance
            FROM account_balance_history
            WHERE account_key = 'accounts_cards'
            ORDER BY recorded_at DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if row is None or row[0] is None:
            return 0
        return int(row[0])
    except Exception:
        return 0


def sync_card_excel() -> None:
    if not SYNCED_CARD_EXCEL.exists():
        log(f"Card Excel source missing: {SYNCED_CARD_EXCEL}")
        return
    try:
        shutil.copy2(SYNCED_CARD_EXCEL, FILE_CARD_EXCEL)
        log(f"Copied card Excel: {SYNCED_CARD_EXCEL.name}")
    except Exception as exc:
        log(f"Failed to copy card Excel: {exc}")


def apply_card_balance(cur: Any, last_update: dict[str, float], accounts: Accounts) -> bool:
    sync_card_excel()

    refresh_cards = should_refresh_cards(last_update)
    if refresh_cards:
        log("Refreshing card balances from Excel")
        cards.main(cur, filepath_exel=str(FILE_CARD_EXCEL))
        last_update["cards"] = FILE_CARD_EXCEL.stat().st_mtime
    else:
        log("Using latest stored card balance")

    card_balance = fetch_latest_card_balance(cur)
    merge_snapshot(
        accounts,
        build_special_snapshot(
            account_key="accounts_cards",
            balance=card_balance,
            source="cards",
        ),
    )
    return refresh_cards


def validate_total(accounts: Accounts) -> None:
    total = sum(snapshot.balance for snapshot in accounts.values())
    log(f"Sanity total: {total:,}")
    if total <= 150_000_000:
        raise ValueError("Total below 150,000,000; aborting as suspicious")


def filter_accounts(accounts: Accounts, exclusions: ExclusionRules) -> Accounts:
    return {
        key: snapshot
        for key, snapshot in accounts.items()
        if not exclusions.excludes(key, snapshot.company)
    }


def build_account_rows(accounts: Accounts, is_active: bool) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for snapshot in accounts.values():
        rows.append(
            (
                snapshot.account_key,
                snapshot.company,
                snapshot.account_type,
                snapshot.name or snapshot.account_key,
                snapshot.memo,
                snapshot.is_special or snapshot.account_key in KNOWN_SPECIAL_KEYS,
                is_active,
            )
        )
    return rows


def upsert_accounts(cur: Any, accounts: Accounts, is_active: bool = True) -> None:
    rows = build_account_rows(accounts, is_active)
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


def fetch_latest_balances(cur: Any, account_keys: list[str]) -> dict[str, int]:
    if not account_keys:
        return {}

    cur.execute(
        """
        SELECT DISTINCT ON (account_key)
            account_key,
            balance
        FROM account_balance_history
        WHERE account_key = ANY(%s)
        ORDER BY account_key, recorded_at DESC
        """,
        (account_keys,),
    )
    return {account_key: int(balance) for account_key, balance in cur.fetchall()}


def build_history_rows(
    accounts: Accounts,
    latest_balances: dict[str, int],
    recorded_at: dt.datetime,
) -> list[tuple[str, dt.datetime, int, str]]:
    rows: list[tuple[str, dt.datetime, int, str]] = []
    for snapshot in accounts.values():
        previous_balance = latest_balances.get(snapshot.account_key)
        if previous_balance == snapshot.balance:
            continue
        rows.append((snapshot.account_key, recorded_at, snapshot.balance, snapshot.source))
    return rows


def insert_account_history(cur: Any, accounts: Accounts, recorded_at: dt.datetime) -> None:
    if not accounts:
        return

    latest_balances = fetch_latest_balances(cur, list(accounts))
    rows = build_history_rows(accounts, latest_balances, recorded_at)
    if not rows:
        log("No account balance changes detected; skipped history insert.")
        return

    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO account_balance_history (
            account_key, recorded_at, balance, source
        ) VALUES %s
        ON CONFLICT (account_key, recorded_at) DO UPDATE SET
            balance = EXCLUDED.balance,
            source = EXCLUDED.source
        """,
        rows,
    )


def zero_out_missing_accounts(
    cur: Any,
    current_accounts: Accounts,
    recorded_at: dt.datetime,
    exclusions: ExclusionRules,
) -> None:
    cur.execute(
        """
        SELECT
            a.account_key,
            a.company,
            COALESCE(latest.balance, 0) AS latest_balance
        FROM accounts a
        LEFT JOIN LATERAL (
            SELECT balance
            FROM account_balance_history h
            WHERE h.account_key = a.account_key
            ORDER BY h.recorded_at DESC
            LIMIT 1
        ) AS latest ON TRUE
        ORDER BY a.account_key
        """
    )

    rows_to_insert: list[tuple[str, dt.datetime, int, str]] = []
    zeroed_accounts: list[str] = []
    for account_key, company, latest_balance in cur.fetchall():
        if exclusions.excludes(account_key, company):
            continue
        if account_key in current_accounts:
            continue
        if latest_balance is None or int(latest_balance) == 0:
            continue
        rows_to_insert.append((account_key, recorded_at, 0, "zero_out"))
        zeroed_accounts.append(account_key)

    if rows_to_insert:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO account_balance_history (
                account_key, recorded_at, balance, source
            ) VALUES %s
            ON CONFLICT (account_key, recorded_at) DO UPDATE SET
                balance = EXCLUDED.balance,
                source = EXCLUDED.source
            """,
            rows_to_insert,
        )

    if zeroed_accounts:
        cur.execute(
            "UPDATE accounts SET is_active = FALSE, updated_at = now() WHERE account_key = ANY(%s)",
            (zeroed_accounts,),
        )


def chunked(rows: list[tuple[Any, ...]], size: int = 2000) -> list[list[tuple[Any, ...]]]:
    return [rows[index : index + size] for index in range(0, len(rows), size)]


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
    current_timestamp: dt.datetime | None = None
    portfolio_rows: list[tuple[dt.datetime, int]] = []

    for account_key, recorded_at, balance in progress_iter(cur.fetchall(), desc="portfolio_totals"):
        if current_timestamp is not None and recorded_at != current_timestamp:
            portfolio_rows.append((current_timestamp, total))
        balance_int = int(balance)
        previous = latest_by_account.get(account_key, 0)
        total += balance_int - previous
        latest_by_account[account_key] = balance_int
        current_timestamp = recorded_at

    if current_timestamp is not None:
        portfolio_rows.append((current_timestamp, total))
    return portfolio_rows


def extend_portfolio_rows_to_now(portfolio_rows: list[tuple[dt.datetime, int]]) -> list[tuple[dt.datetime, int]]:
    if not portfolio_rows:
        return portfolio_rows

    as_of = now_local().replace(microsecond=0)
    latest_recorded_at, latest_total = portfolio_rows[-1]
    if latest_recorded_at >= as_of:
        return portfolio_rows
    return [*portfolio_rows, (as_of, latest_total)]


def build_daydiff_rows(portfolio_rows: list[tuple[dt.datetime, int]]) -> list[tuple[dt.date, int]]:
    timezone = get_app_timezone()
    day_totals: dict[dt.date, int] = {}
    for recorded_at, balance in portfolio_rows:
        day_totals[recorded_at.astimezone(timezone).date()] = int(balance)

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


def build_monthdiff_rows(portfolio_rows: list[tuple[dt.datetime, int]]) -> list[tuple[dt.date, int]]:
    timezone = get_app_timezone()
    month_totals: dict[tuple[int, int], int] = {}
    month_labels: dict[tuple[int, int], dt.date] = {}
    for recorded_at, balance in portfolio_rows:
        local_stamp = recorded_at.astimezone(timezone)
        month_key = (local_stamp.year, local_stamp.month)
        month_totals[month_key] = int(balance)
        month_labels[month_key] = local_stamp.date()

    result: list[tuple[dt.date, int]] = []
    previous_total: int | None = None
    for month_key in sorted(month_totals):
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


def build_run_context() -> RunContext:
    if not FILE_KB_PICKLE.is_file():
        raise FileNotFoundError(f"{FILE_KB_PICKLE} is missing")

    last_update = load_last_update_state()
    last_update["KB"] = FILE_KB_PICKLE.stat().st_mtime
    return RunContext(
        recorded_at=now_local().replace(microsecond=0),
        last_update=last_update,
        exclusions=load_exclusion_rules(),
    )


def build_accounts(cur: Any, context: RunContext) -> Accounts:
    log("=== Load KB openbank data ===")
    accounts = load_openbank_accounts(context.exclusions)

    log("=== Apply manual inputs ===")
    apply_manual_inputs_from_db(cur, accounts)

    validate_total(accounts)

    log("=== Apply card balance ===")
    apply_card_balance(cur, context.last_update, accounts)
    return accounts


def persist_accounts(cur: Any, context: RunContext, accounts: Accounts) -> Accounts:
    filtered_accounts = filter_accounts(accounts, context.exclusions)
    if not filtered_accounts:
        raise RuntimeError("No accounts remained after filtering")

    log("=== Upsert account metadata ===")
    upsert_accounts(cur, filtered_accounts, is_active=True)

    log("=== Insert account history ===")
    insert_account_history(cur, filtered_accounts, context.recorded_at)
    zero_out_missing_accounts(cur, filtered_accounts, context.recorded_at, context.exclusions)
    return filtered_accounts


def run_kb_pipeline() -> bool | tuple[str, Exception]:
    connection = None
    cur = None
    try:
        log("=== Prepare run context ===")
        context = build_run_context()

        connection = get_db_connection()
        cur = connection.cursor()
        ensure_normalized_schema(cur)

        accounts = build_accounts(cur, context)
        filtered_accounts = persist_accounts(cur, context, accounts)

        log("=== Rebuild portfolio summaries ===")
        rebuild_portfolio_summaries(cur)

        log("=== Record crawl heartbeat ===")
        write_crawl_heartbeat(
            cur,
            recorded_at=context.recorded_at,
            account_count=len(filtered_accounts),
        )

        connection.commit()

        if not save_last_update_state(context.last_update):
            raise RuntimeError(f"Failed to save {FILE_LAST_UPDATE}")

        log("=== Complete ===")
        return True
    except Exception as exc:
        if connection is not None:
            connection.rollback()
        return ("ERROR", exc)
    finally:
        if cur is not None:
            cur.close()
        if connection is not None:
            connection.close()


def KB_main() -> bool | tuple[str, Exception]:
    return run_kb_pipeline()


if __name__ == "__main__":
    result = KB_main()
    if isinstance(result, tuple) and result[0] == "ERROR":
        print(f"\n[ERROR] KB_main failed: {result[1]!r}")
        raise SystemExit(1)
    print("\n=== exit ===")
