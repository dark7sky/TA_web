from __future__ import annotations

import datetime as dt
import os
import pickle
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import cards
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from tqdm import tqdm

import logger
from KB_web import all_datas

load_dotenv()

logs = logger.logger(os.path.basename(__file__).split(".")[0])


def log(msg: str) -> None:
    logs.msg(msg)


FILE_LAST_UPDATE = ".last_update.pickle"
FILE_LAST_UPDATE_BK = ".last_update_bk.pickle"
FILE_KB_PICKLE = "KB.pickle"
FILE_KBSELF_PICKLE = "KBself.pickle"
FILE_CREON = "CREON.pickle"
FILE_CARD_PICKLE = "cards.pickle"
FILE_CARD_EXCEL = "카드통합.xlsx"
MANUAL_INPUTS_TABLE = "manual_inputs"
APP_TIMEZONE_ENV = "APP_TIMEZONE"
CREON_ACCOUNT_NUMBER = os.getenv("CREON_ACCOUNT_NUMBER")
EXCEPTION_BANKS_ENV = "EXCEPTION_BANKS"
EXCEPTION_ACCOUNTS_ENV = "EXCEPTION_ACCOUNTS"
USE_TQDM = True
KNOWN_SPECIAL_KEYS = {
    "accounts_cards",
    "toss",
    "debt",
    "insurance",
    "lab_private",
    "외화",
    "지역화폐",
    "장기수선충당금",
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
    raw = os.getenv(key, "")
    if raw in (None, ""):
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def load_exceptions() -> dict[str, list[str]]:
    return {
        "bank": parse_env_csv(EXCEPTION_BANKS_ENV),
        "accounts": parse_env_csv(EXCEPTION_ACCOUNTS_ENV),
    }


def lst_udt(operation: str = "read", last_update: dict[str, float] | None = None) -> dict[str, float] | bool:
    last_update = last_update or {}
    if operation == "read":
        try:
            with open(FILE_LAST_UPDATE, "rb") as file_obj:
                return pickle.load(file_obj)
        except FileNotFoundError:
            log(f"{FILE_LAST_UPDATE} not found; using defaults.")
            return {"KB": 0.0}
        except pickle.UnpicklingError:
            log(f"{FILE_LAST_UPDATE} is corrupt; loading backup.")
            with open(FILE_LAST_UPDATE_BK, "rb") as file_obj:
                return pickle.load(file_obj)

    if operation == "save":
        try:
            with open(FILE_LAST_UPDATE, "wb") as file_obj:
                pickle.dump(last_update, file_obj)
            return True
        except Exception as exc:
            log(f"Error saving {FILE_LAST_UPDATE}: {exc}")
            return False

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
    if additive:
        existing.balance += snapshot.balance
    else:
        existing.balance = snapshot.balance
    if snapshot.source:
        existing.source = snapshot.source


def normalize_manual_key(raw_key: str) -> tuple[str, str | None]:
    key_aliases = {
        "랩비": "lab_private",
        "toss증권": "toss",
        "대출": "debt",
        "보험": "insurance",
    }
    if "@" in raw_key:
        head, *tail = raw_key.split("@")
        return key_aliases.get(head, head), "".join(tail) or None
    return key_aliases.get(raw_key, raw_key), None


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


def load_openbank_accounts(exceptions: dict[str, list[str]]) -> Accounts:
    with open(FILE_KB_PICKLE, "rb") as file_obj:
        loaded = pickle.load(file_obj)

    if not isinstance(loaded, list):
        raise TypeError(f"{FILE_KB_PICKLE} must contain a list of rows")

    accounts: Accounts = {}
    for row in loaded:
        if not isinstance(row, (list, tuple)) or len(row) < 5:
            log(f"Skip malformed KB row: {row!r}")
            continue
        company = str(row[0]).strip()
        account_type = str(row[1]).strip()
        account_key = str(row[2]).strip()
        name = str(row[3]).strip() or account_key
        balance = parse_int_value(row[4])
        if not account_key or balance is None:
            log(f"Skip KB row with invalid account or balance: {row!r}")
            continue
        if company in exceptions.get("bank", []):
            continue
        if account_key in exceptions.get("accounts", []):
            continue
        merge_snapshot(
            accounts,
            AccountSnapshot(
                account_key=account_key,
                balance=balance,
                company=company,
                account_type=account_type,
                name=name,
                source="kb_openbank",
            ),
        )
    return accounts


def load_kb_securities(accounts: Accounts) -> None:
    try:
        with open(FILE_KBSELF_PICKLE, "rb") as file_obj:
            loaded = pickle.load(file_obj)
    except FileNotFoundError:
        log(f"{FILE_KBSELF_PICKLE} not found; skipping KB securities.")
        return

    if not isinstance(loaded, dict):
        raise TypeError(f"{FILE_KBSELF_PICKLE} must contain a dict")

    for raw_key in all_datas.get("KBaccounts", []):
        account_key = str(raw_key).strip()
        if not account_key:
            continue
        raw_balance = loaded.get(account_key, 0)
        balance = parse_int_value(raw_balance)
        if balance is None:
            log(f"Skip KB securities row with invalid balance: {account_key}={raw_balance!r}")
            continue
        if account_key not in loaded:
            log(f"KB stock account not found ({account_key})")
        existing = accounts.get(account_key)
        merge_snapshot(
            accounts,
            AccountSnapshot(
                account_key=account_key,
                balance=balance,
                company="KB증권" if existing is None else "",
                account_type="종합위탁" if existing is None else "",
                name="KB주식투자계좌" if existing is None else "",
                source="kb_securities",
            ),
        )


def apply_manual_inputs_from_db(cur: Any, accounts: Accounts) -> None:
    try:
        cur.execute(
            f"SELECT key_name, value FROM \"{MANUAL_INPUTS_TABLE}\" ORDER BY key_name"
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


def apply_creon_balance(accounts: Accounts) -> None:
    if CREON_ACCOUNT_NUMBER in (None, ""):
        raise RuntimeError("CREON_ACCOUNT_NUMBER is missing in the environment")
    try:
        with open(FILE_CREON, "rb") as file_obj:
            raw_balance = pickle.load(file_obj)
    except FileNotFoundError:
        raw_balance = 0

    balance = parse_int_value(raw_balance)
    if balance is None:
        raise ValueError(f"{FILE_CREON} does not contain an int-like value")

    existing = accounts.get(CREON_ACCOUNT_NUMBER)
    merge_snapshot(
        accounts,
        AccountSnapshot(
            account_key=CREON_ACCOUNT_NUMBER,
            balance=balance,
            company="" if existing else "CREON",
            account_type="" if existing else "Brokerage",
            name="" if existing else "CREON",
            source="creon",
        ),
    )


def should_refresh_cards(last_update: dict[str, float]) -> bool:
    if not os.path.isfile(FILE_CARD_EXCEL):
        return False
    if "cards" not in last_update:
        return True
    if not os.path.isfile(FILE_CARD_PICKLE):
        return True
    card_pickle_date = dt.date.fromtimestamp(os.path.getmtime(FILE_CARD_PICKLE))
    if card_pickle_date != now_local().date():
        return True
    return os.path.getmtime(FILE_CARD_EXCEL) != last_update["cards"]


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


def apply_card_balance(cur: Any, last_update: dict[str, float], accounts: Accounts) -> bool:
    refresh_cards = should_refresh_cards(last_update)
    if refresh_cards:
        log("Refreshing card balances from Excel")
        cards.main(cur, filepath_exel=FILE_CARD_EXCEL)
        last_update["cards"] = os.path.getmtime(FILE_CARD_EXCEL)
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


def filter_accounts(accounts: Accounts, exceptions: dict[str, list[str]]) -> Accounts:
    excluded_accounts = set(exceptions.get("accounts", []))
    excluded_banks = set(exceptions.get("bank", []))
    return {
        key: snapshot
        for key, snapshot in accounts.items()
        if key not in excluded_accounts and snapshot.company not in excluded_banks
    }


def upsert_accounts(cur: Any, accounts: Accounts, is_active: bool = True) -> None:
    rows = []
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


def insert_account_history(cur: Any, accounts: Accounts, recorded_at: dt.datetime) -> None:
    rows = [
        (snapshot.account_key, recorded_at, snapshot.balance, snapshot.source)
        for snapshot in accounts.values()
    ]
    if not rows:
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
    exceptions: dict[str, list[str]],
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

    excluded_accounts = set(exceptions.get("accounts", []))
    excluded_banks = set(exceptions.get("bank", []))
    rows_to_insert = []
    zeroed_accounts = []
    for account_key, company, latest_balance in cur.fetchall():
        if account_key in excluded_accounts or company in excluded_banks:
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


def chunked(rows: list[tuple], size: int = 2000) -> list[list[tuple]]:
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
    current_ts: dt.datetime | None = None
    portfolio_rows: list[tuple[dt.datetime, int]] = []

    for account_key, recorded_at, balance in progress_iter(cur.fetchall(), desc="portfolio_totals"):
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


def build_daydiff_rows(portfolio_rows: list[tuple[dt.datetime, int]]) -> list[tuple[dt.date, int]]:
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


def build_monthdiff_rows(portfolio_rows: list[tuple[dt.datetime, int]]) -> list[tuple[dt.date, int]]:
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
    daydiff_rows = build_daydiff_rows(portfolio_rows)
    monthdiff_rows = build_monthdiff_rows(portfolio_rows)

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


def KB_main() -> bool | tuple[str, Exception]:
    connection = None
    try:
        log("=== Prepare run context ===")
        recorded_at = now_local().replace(microsecond=0)
        last_update = lst_udt(operation="read")
        exceptions = load_exceptions()

        if not os.path.isfile(FILE_KB_PICKLE):
            raise FileNotFoundError(f"{FILE_KB_PICKLE} is missing")
        last_update["KB"] = os.path.getmtime(FILE_KB_PICKLE)

        log("=== Load KB openbank data ===")
        accounts = load_openbank_accounts(exceptions)
        load_kb_securities(accounts)

        connection = get_db_connection()
        cur = connection.cursor()
        ensure_normalized_schema(cur)

        log("=== Apply manual inputs ===")
        apply_manual_inputs_from_db(cur, accounts)

        log("=== Apply CREON balance ===")
        apply_creon_balance(accounts)

        validate_total(accounts)

        log("=== Apply card balance ===")
        apply_card_balance(cur, last_update, accounts)

        filtered_accounts = filter_accounts(accounts, exceptions)
        if not filtered_accounts:
            raise RuntimeError("No accounts remained after filtering")

        log("=== Upsert account metadata ===")
        upsert_accounts(cur, filtered_accounts, is_active=True)

        log("=== Insert account history ===")
        insert_account_history(cur, filtered_accounts, recorded_at)
        zero_out_missing_accounts(cur, filtered_accounts, recorded_at, exceptions)

        log("=== Rebuild portfolio summaries ===")
        rebuild_portfolio_summaries(cur)

        connection.commit()
        cur.close()
        connection.close()
        connection = None

        if not lst_udt(operation="save", last_update=last_update):
            raise RuntimeError(f"Failed to save {FILE_LAST_UPDATE}")

        log("=== Complete ===")
        return True
    except Exception as exc:
        if connection is not None:
            connection.rollback()
            connection.close()
        return ("ERROR", exc)


if __name__ == "__main__":
    result = KB_main()
    if isinstance(result, tuple) and result[0] == "ERROR":
        print(f"\n[ERROR] KB_main failed: {result[1]!r}")
        raise SystemExit(1)
    print("\n=== exit ===")
