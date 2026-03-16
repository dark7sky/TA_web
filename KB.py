"""KB.py ??Total Account Tracker
Fetches and persists account balances from KB banking/securities pickles,
manual entries, card data, and CREON, into a local PostgreSQL database.

Last modified: 2026-03-04 (optimized)
"""
import os
import cards
import pickle
import psycopg2
import psycopg2.extras
import io
import csv
from typing import Any
from dotenv import load_dotenv
load_dotenv()
import datetime
import functools
from typing import Callable, Any, Dict, Optional, Tuple, Iterable

from tqdm import tqdm
from KB_web import all_datas
import logger

print("Last modified: 2026-03-04 with Antigravity - Claude Sonnet 4.6")
# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logs = logger.logger(os.path.basename(__file__).split(".")[0])


def log(msg: str) -> None:
    logs.msg(msg)


# ---------------------------------------------------------------------------
# File / path constants
# ---------------------------------------------------------------------------
FILE_LAST_UPDATE    = ".last_update.pickle"
FILE_LAST_UPDATE_BK = ".last_update_bk.pickle"
FILE_KB_PICKLE      = "KB.pickle"
FILE_KBSELF_PICKLE  = "KBself.pickle"
FILE_MANUAL         = "manual.pickle"
FILE_CREON          = "CREON.pickle"
CREON_ACCOUNT_NUMBER = os.getenv("CREON_ACCOUNT_NUMBER")
EXCEPTION_BANKS_ENV = "EXCEPTION_BANKS"
EXCEPTION_ACCOUNTS_ENV = "EXCEPTION_ACCOUNTS"

# Tables that are not per-account ledgers.
NON_ACCOUNT_TABLES = {
    "accounts_balance",
    "accounts_daydiff",
    "accounts_diff",
    "accounts_info",
    "accounts_monthdiff",
    "manual_inputs",
    "system_settings",
}

filepath_cardexcel  = "移대뱶?듯빀.xlsx"

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------
Accounts   = Dict[str, Dict[str, Any]]
StepResult = Tuple[str, Exception]

# ---------------------------------------------------------------------------
# Global DB connection (managed by db_decorator)
# ---------------------------------------------------------------------------
db_con: Optional[Any] = None

USE_TQDM = True

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def progress_iter(iterable: Iterable, desc: str = "") -> Iterable:
    """Wrap *iterable* with tqdm when USE_TQDM is True."""
    return tqdm(iterable, desc=desc) if USE_TQDM else iterable


def required_env(key: str) -> str:
    value = os.getenv(key)
    if value in (None, ""):
        raise RuntimeError(f"Required env var missing: {key}")
    return value


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


def lst_udt(
    operation: str = "read",
    last_update: Optional[Dict[str, float]] = None,
) -> Dict[str, float] | bool:
    """Read or save `.last_update.pickle`.

    Args:
        operation: ``"read"`` (default) or ``"save"``.
        last_update: Dict to persist when *operation* is ``"save"``.

    Returns:
        dict on read, bool on save.
    """
    if last_update is None:
        last_update = {}

    if operation == "read":
        try:
            with open(FILE_LAST_UPDATE, "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            log(f"{FILE_LAST_UPDATE} not found ??using default.")
            return {"KB": float(0)}
        except pickle.UnpicklingError:
            log(f"{FILE_LAST_UPDATE} corrupt ??loading backup.")
            with open(FILE_LAST_UPDATE_BK, "rb") as f:
                return pickle.load(f)

    elif operation == "save":
        try:
            with open(FILE_LAST_UPDATE, "wb") as f:
                pickle.dump(last_update, f)
            return True
        except Exception as e:
            log(f"Error saving {FILE_LAST_UPDATE}: {e}")
            return False

    return False


# ---------------------------------------------------------------------------
# Database decorator
# ---------------------------------------------------------------------------

def db_decorator():
    """Decorator factory: opens (or reuses) a global SQLite cursor and
    injects it as *cur* keyword argument into the wrapped function."""
    global db_con
    if db_con is None:
        db_con = psycopg2.connect(
            host=required_env("DB_HOST"),
            database=required_env("DB_NAME"),
            user=required_env("DB_USER"),
            password=required_env("DB_PASSWORD"),
            port=os.getenv("DB_PORT", "5432"),
            sslmode='disable'
        )
        db_con.set_session(autocommit=True)
    try:
        cur = db_con.cursor()
    except Exception:
        db_con = psycopg2.connect(
            host=required_env("DB_HOST"),
            database=required_env("DB_NAME"),
            user=required_env("DB_USER"),
            password=required_env("DB_PASSWORD"),
            port=os.getenv("DB_PORT", "5432"),
            sslmode='disable'
        )
        db_con.set_session(autocommit=True)
        cur = db_con.cursor()

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, cur=cur, **kwargs)
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Step functions
# ---------------------------------------------------------------------------

@logger.with_logging(logs)
def KB_prepare() -> tuple[str, dict[str, float], dict] | tuple[str, Exception]:
    """Prepare *tnow*, *last_update*, and *exceptions*.

    Returns:
        ``(tnow, last_update, exceptions)`` or ``("ERROR", exc)``.
    """
    try:
        tnow = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        last_update = lst_udt(operation="read")
        exceptions = load_exceptions()
        return tnow, last_update, exceptions
    except Exception as e:
        return "ERROR", e


@logger.with_logging(logs)
def kbstock_openbank(tnow: str, exceptions: dict) -> dict | tuple[str, Exception]:
    """Parse ``KB.pickle`` (open-banking data) and build *accounts* dict.

    Args:
        tnow: Timestamp string for this run.
        exceptions: Banks/accounts to skip.

    Returns:
        accounts dict or ``("ERROR", exc)``.
    """
    accounts: dict = {}
    try:
        with open(FILE_KB_PICKLE, "rb") as f:
            lines = pickle.load(f)
        for line in lines:
            if line[0] in exceptions.get("bank", []):
                continue
            if line[2] in exceptions.get("accounts", []):
                continue
            accounts[line[2]] = {
                "company": line[0],
                "type":    line[1],
                "name":    line[3],
                "balance": {tnow: line[4]},
            }
    except Exception as e:
        return "ERROR", e
    return accounts


@logger.with_logging(logs)
def kbstock(tnow: str, accounts: dict) -> dict | tuple[str, Exception]:
    """Merge KB-securities balances from ``KBself.pickle`` into *accounts*.

    Args:
        tnow: Timestamp string for this run.
        accounts: Existing accounts dict.

    Returns:
        Updated accounts dict or ``("ERROR", exc)``.
    """
    try:
        with open(FILE_KBSELF_PICKLE, "rb") as f:
            kbselfresult = pickle.load(f)
        for accnum in all_datas["KBaccounts"]:
            balance = kbselfresult.get(accnum, 0)
            if balance == 0:
                log(f"KB-stock account not found ({accnum})")
            if accnum in accounts:
                accounts[accnum]["balance"][tnow] = balance
            else:
                accounts[accnum] = {
                    "company": "KB利앷텒",
                    "type":    "醫낇빀?꾪긽",
                    "name":    "KB二쇱떇?ъ옄怨꾩쥖",
                    "balance": {tnow: balance},
                }
    except FileNotFoundError:
        log("KBself.pickle not found ??skipping KB-securities.")
    except Exception as e:
        return "ERROR", e
    log("=== KB 利앷텒 update ===")
    return accounts


@logger.with_logging(logs)
def pickle_read(
    tnow: str, accounts: dict, fp: str, accname: str
) -> dict | tuple[str, Exception]:
    """Read a single-value pickle and store it as an account balance.

    Args:
        tnow: Timestamp string for this run.
        accounts: Existing accounts dict.
        fp: Path to the pickle file.
        accname: Key to use in *accounts*.

    Returns:
        Updated accounts dict or ``("ERROR", exc)``.
    """
    try:
        with open(fp, "rb") as f:
            lab = int(pickle.load(f))
    except FileNotFoundError:
        log(f"{fp} not found ??defaulting to 0.")
        lab = 0
    except Exception as e:
        return "ERROR", e
    accounts[accname] = {"balance": {tnow: lab}}
    return accounts


# ---------------------------------------------------------------------------
# DB: account info
# ---------------------------------------------------------------------------

@logger.with_logging(logs)
@db_decorator()
def cards_to_accounts(
    cur: Any, tnow: str, accounts: dict
) -> dict | tuple[str, Exception]:
    """Run card analysis and reflect the result in *accounts*.

    Args:
        cur: DB cursor (injected by db_decorator).
        tnow: Timestamp string for this run.
        accounts: Existing accounts dict.

    Returns:
        Updated accounts dict or ``("ERROR", exc)``.
    """
    log("=== 移대뱶 寃곗젣 ?붽퀬 ?낅뜲?댄듃 ===")
    try:
        cards.main(cur, filepath_exel=filepath_cardexcel)
    except Exception as e:
        log(f"cards.main ?먮윭: {e}")
        return "ERROR", e

    cur.execute(
        "SELECT balance FROM accounts_cards "
        "WHERE date IN (SELECT max(date) FROM accounts_cards)"
    )
    temp = cur.fetchone()
    accounts["accounts_cards"] = {"balance": {tnow: int(temp[0])}}
    return accounts


@logger.with_logging(logs)
@db_decorator()
def last_card(cur: Any, tnow: str, accounts: dict) -> dict:
    """Load the latest card balance without re-parsing the Excel file."""
    cur.execute(
        "SELECT balance FROM accounts_cards "
        "WHERE date IN (SELECT max(date) FROM accounts_cards)"
    )
    val = cur.fetchone()
    accounts["accounts_cards"] = {"balance": {tnow: int(val[0])}}
    return accounts


@logger.with_logging(logs)
@db_decorator()
def info_update(cur: Any, accounts: dict) -> None:
    """Upsert account metadata into ``accounts_info`` table.

    Args:
        cur: DB cursor (injected by db_decorator).
        accounts: Accounts dict with metadata keys.
    """
    try:
        cur.execute("SELECT account_number FROM accounts_info LIMIT 1")
    except (psycopg2.OperationalError, psycopg2.errors.UndefinedTable):
        cur.execute(
            "CREATE TABLE accounts_info("
            "account_number TEXT NOT NULL,"
            "company TEXT,"
            "type TEXT,"
            "name TEXT,"
            "memo TEXT,"
            "PRIMARY KEY(account_number))"
        )

    insert_data = []
    for acc_num, acc_data in accounts.items():
        acc_data.setdefault("memo", "")
        insert_data.append((
            acc_num,
            acc_data.get("company", ""),
            acc_data.get("type", ""),
            acc_data.get("name", ""),
            acc_data.get("memo", ""),
        ))

    if insert_data:
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO accounts_info (account_number, company, type, name, memo) VALUES %s ON CONFLICT (account_number) DO NOTHING",
            insert_data
        )


@logger.with_logging(logs)
@db_decorator()
def balance_update(
    cur: Any, accounts: dict, tnow: str, exceptions: dict
) -> None:
    """Persist per-account balance records and zero-out closed accounts.

    Args:
        cur: DB cursor (injected by db_decorator).
        accounts: Accounts dict.
        tnow: Timestamp string for this run.
        exceptions: Banks/accounts excluded from processing.
    """
    # Pre-fetch all existing tables once ??avoids N individual SELECT-EXISTS queries
    cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public'")
    existing_tables = {row[0] for row in cur.fetchall()} - NON_ACCOUNT_TABLES

    # Never treat non-account tables as account ledgers
    accounts = {k: v for k, v in accounts.items() if k not in NON_ACCOUNT_TABLES}

    # Create missing tables in a single loop (no individual SELECT per account)
    for acc_num in accounts.keys():
        if acc_num not in existing_tables:
            cur.execute(
                f'CREATE TABLE "{acc_num}" '
                f'(date TEXT NOT NULL, balance INTEGER, PRIMARY KEY(date))'
            )

    # Bulk Insert values
    for acc_num, acc_data in tqdm(accounts.items(), desc="balance_update"):
        insert_data = list(acc_data["balance"].items())
        if insert_data:
            psycopg2.extras.execute_values(
                cur,
                f'INSERT INTO "{acc_num}" (date, balance) VALUES %s ON CONFLICT (date) DO NOTHING',
                insert_data
            )

    # Zero-out accounts that disappeared from the feed
    cur.execute("SELECT account_number, company FROM accounts_info")
    all_info = cur.fetchall()
    exc_accounts = set(exceptions.get("accounts", []))
    exc_banks    = set(exceptions.get("bank", []))

    zero_out_tasks = []
    for acc_num, company in tqdm(all_info, desc="zero-out check"):
        if acc_num in exc_accounts or company in exc_banks:
            continue
        if acc_num not in accounts and acc_num in existing_tables:
            try:
                cur.execute(
                    f'SELECT balance FROM "{acc_num}" '
                    f'ORDER BY date DESC LIMIT 1'
                )
                last = cur.fetchone()
                if last and int(last[0]) != 0:
                    zero_out_tasks.append((acc_num, tnow))
            except (psycopg2.OperationalError, psycopg2.errors.UndefinedTable):
                pass

    for acc_num, date_val in zero_out_tasks:
        try:
            cur.execute(
                f'INSERT INTO "{acc_num}" (date, balance) VALUES (%s, %s) ON CONFLICT (date) DO NOTHING',
                (date_val, 0)
            )
        except Exception:
            pass


@logger.with_logging(logs)
@db_decorator()
def accounts_balance_update(cur: Any, accounts: dict) -> None:
    """Append total balance and diff rows for the current snapshot.

    Args:
        cur: DB cursor (injected by db_decorator).
        accounts: Accounts dict.
    """
    # Ensure summary tables exist
    try:
        cur.execute("SELECT max(date) FROM accounts_balance")
    except (psycopg2.OperationalError, psycopg2.errors.UndefinedTable):
        cur.execute(
            "CREATE TABLE accounts_balance("
            "date TEXT, balance INTEGER, PRIMARY KEY(date))"
        )
    try:
        cur.execute("SELECT max(date) FROM accounts_diff")
    except (psycopg2.OperationalError, psycopg2.errors.UndefinedTable):
        cur.execute(
            "CREATE TABLE accounts_diff("
            "date TEXT, balance INTEGER, PRIMARY KEY(date))"
        )

    # Fetch last known total once (outside the loop)
    try:
        cur.execute(
            "SELECT balance FROM accounts_balance "
            "ORDER BY date DESC LIMIT 1"
        )
        row = cur.fetchone()
        last_balance = int(row[0]) if row else 0
    except (psycopg2.OperationalError, psycopg2.errors.UndefinedTable):
        last_balance = 0

    # Exclude non-account tables from total calculations
    accounts = {k: v for k, v in accounts.items() if k not in NON_ACCOUNT_TABLES}

    balance_insert_data = []
    diff_insert_data = []

    first_acc = next(iter(accounts))
    for date_key in accounts[first_acc]["balance"]:
        total = sum(
            acc_data["balance"].get(date_key, 0)
            for acc_data in accounts.values()
        )
        balance_insert_data.append((date_key, total))
        diff_insert_data.append((date_key, total - last_balance))
        last_balance = total

    try:
        if balance_insert_data:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO accounts_balance (date, balance) VALUES %s ON CONFLICT (date) DO NOTHING",
                balance_insert_data
            )
        if diff_insert_data:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO accounts_diff (date, balance) VALUES %s ON CONFLICT (date) DO NOTHING",
                diff_insert_data
            )
    except Exception as e:
        log(f"Accounts 醫낇빀 ?낅뜲?댄듃 ?ㅽ뙣: {e}")
        raise


@logger.with_logging(logs)
@db_decorator()
def refresh_accounts_balance(cur: Any) -> None:
    """Fully recompute ``accounts_balance`` and ``accounts_diff`` from scratch.

    Pre-fetches all per-account data into memory to avoid N횞M PostgreSQL
    round-trips (one SELECT per day 횞 table).
    """
    cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public'")
    table_list = [row[0] for row in cur.fetchall() if row[0] not in NON_ACCOUNT_TABLES]

    # --- Pre-fetch all balances into memory (keyed by table name) ---
    # table_data[tab] = sorted list of (date, balance) tuples
    table_data: Dict[str, list] = {}
    all_dates: set = set()
    for tab in table_list:
        cur.execute(f'SELECT date, balance FROM "{tab}" ORDER BY date')
        rows = cur.fetchall()
        table_data[tab] = rows
        all_dates.update(r[0] for r in rows)

    days = sorted(all_dates)

    # Build a cumulative pointer per table.
    result = []
    pointers: Dict[str, int] = {tab: 0 for tab in table_list}
    last_vals: Dict[str, int] = {tab: 0 for tab in table_list}

    log("refresh_accounts_balance: computing totals...")
    for day in tqdm(days):
        for tab in table_list:
            rows = table_data[tab]
            p = pointers[tab]
            while p < len(rows) and rows[p][0] <= day:
                last_vals[tab] = rows[p][1]
                p += 1
            pointers[tab] = p
        result.append((day, sum(last_vals.values())))

    cur.execute("DELETE FROM accounts_balance")
    cur.execute("DELETE FROM accounts_diff")

    log("refresh_accounts_balance: writing results using COPY...")
    pval = 0
    balance_buf = io.StringIO()
    diff_buf = io.StringIO()
    
    balance_writer = csv.writer(balance_buf, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
    diff_writer = csv.writer(diff_buf, delimiter='\t', quoting=csv.QUOTE_MINIMAL)

    for n, (day, total) in enumerate(tqdm(result)):
        diff = 0 if n == 0 else total - pval
        pval = total
        balance_writer.writerow([day, total])
        diff_writer.writerow([day, diff])

    balance_buf.seek(0)
    diff_buf.seek(0)

    try:
        cur.copy_from(balance_buf, 'accounts_balance', sep='\t', columns=['date', 'balance'])
        cur.copy_from(diff_buf, 'accounts_diff', sep='\t', columns=['date', 'balance'])
    except Exception as e:
        log(f"COPY Error in refresh_accounts_balance: {e}")
        pass


@logger.with_logging(logs)
@db_decorator()
def refresh_daydiff(cur: Any) -> bool:
    """Populate ``accounts_daydiff`` with daily delta values."""
    try:
        cur.execute(
            'SELECT date FROM "accounts_daydiff" '
            'WHERE date IN (SELECT max(date) FROM "accounts_daydiff")'
        )
    except psycopg2.OperationalError:
        cur.execute(
            'CREATE TABLE "accounts_daydiff" '
            "(date TEXT, balance INTEGER, PRIMARY KEY('date'))"
        )
        return False  # Table just created ??nothing to diff yet

    cur.execute("DELETE FROM accounts_daydiff")

    cur.execute('SELECT min(date) FROM "accounts_balance"')
    start = datetime.datetime.strptime(cur.fetchone()[0], "%Y-%m-%d %H:%M:%S")
    cur.execute('SELECT max(date) FROM "accounts_balance"')
    end = datetime.datetime.strptime(cur.fetchone()[0], "%Y-%m-%d %H:%M:%S")

    start = datetime.datetime.combine(
        datetime.date(start.year, start.month, start.day),
        datetime.time(0, 0, 0),
    )
    end = datetime.datetime.combine(
        datetime.date(end.year, end.month, end.day),
        datetime.time(0, 0, 0),
    )

    # current = 泥??좎쓽 ?ㅼ쓬??湲곗??쇰줈 ?쒖옉 (start ?좎쭨???붽퀬瑜?last_balance濡??ъ슜)
    current = start + datetime.timedelta(days=1)
    cur.execute(
        'SELECT balance FROM accounts_balance WHERE date < %s ORDER BY date DESC LIMIT 1',
        (current.strftime("%Y-%m-%d %H:%M:%S"),),
    )
    last_balance = int(cur.fetchone()[0])

    diffdatas = []
    while True:
        cur.execute(
            'SELECT balance FROM accounts_balance WHERE date < %s ORDER BY date DESC LIMIT 1',
            ((current + datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),),
        )
        row = cur.fetchone()
        cur_balance = int(row[0])
        day_label = current.strftime("%Y-%m-%d")
        diffdatas.append((day_label, cur_balance - last_balance))
        last_balance = cur_balance

        if current >= end:
            break
        current += datetime.timedelta(days=1)

    if diffdatas:
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO accounts_daydiff (date, balance) VALUES %s ON CONFLICT (date) DO NOTHING",
            diffdatas
        )
    return True


@logger.with_logging(logs)
@db_decorator()
def refresh_monthdiff(cur: Any) -> bool:
    """Populate ``accounts_monthdiff`` with month-end delta values."""
    try:
        cur.execute(
            'SELECT date FROM "accounts_monthdiff" '
            'WHERE date IN (SELECT max(date) FROM "accounts_monthdiff")'
        )
    except Exception:
        cur.execute(
            'CREATE TABLE "accounts_monthdiff" '
            "(date TEXT, balance INTEGER, PRIMARY KEY('date'))"
        )
        return False  # Table just created ??nothing to diff yet

    cur.execute("DELETE FROM accounts_monthdiff")

    cur.execute('SELECT min(date) FROM "accounts_balance"')
    start = datetime.datetime.strptime(cur.fetchone()[0], "%Y-%m-%d %H:%M:%S")
    cur.execute('SELECT max(date) FROM "accounts_balance"')
    end = datetime.datetime.strptime(cur.fetchone()[0], "%Y-%m-%d %H:%M:%S")

    start = datetime.datetime.combine(
        datetime.date(start.year, start.month, 1), datetime.time(0, 0, 0)
    )

    # Advance to the first day of the next month
    if start.month < 12:
        current = datetime.datetime.combine(
            datetime.date(start.year, start.month + 1, 1), datetime.time(0, 0, 0)
        )
    else:
        current = datetime.datetime.combine(
            datetime.date(start.year + 1, 1, 1), datetime.time(0, 0, 0)
        )

    cur.execute(
        'SELECT balance FROM accounts_balance WHERE date < %s ORDER BY date DESC LIMIT 1',
        (current.strftime("%Y-%m-%d %H:%M:%S"),),
    )
    last_balance = int(cur.fetchone()[0])

    diffdatas = []
    while True:
        # Advance current to the first day of the following month
        if current.month < 12:
            current = datetime.datetime.combine(
                datetime.date(current.year, current.month + 1, 1),
                datetime.time(0, 0, 0),
            )
        else:
            current = datetime.datetime.combine(
                datetime.date(current.year + 1, 1, 1), datetime.time(0, 0, 0)
            )

        cur.execute(
            'SELECT balance FROM accounts_balance WHERE date < %s ORDER BY date DESC LIMIT 1',
            (current.strftime("%Y-%m-%d %H:%M:%S"),),
        )
        row = cur.fetchone()
        this_balance = int(row[0])

        if current <= end:
            whenisit = str((current - datetime.timedelta(days=1)).date())
        else:
            whenisit = str(end.date())

        diffdatas.append((whenisit, this_balance - last_balance))
        last_balance = this_balance

        if current > end:
            break

    if diffdatas:
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO accounts_monthdiff (date, balance) VALUES %s ON CONFLICT (date) DO NOTHING",
            diffdatas
        )
    return True


@logger.with_logging(logs)
@db_decorator()
def delete_from_a_table(
    cur: Any, tab: str, dates: list[str]
) -> bool | tuple[str, Exception]:
    """Delete multiple date rows from *tab* in a single query.

    Args:
        cur: DB cursor (injected by db_decorator).
        tab: Target table name.
        dates: List of date values to delete.

    Returns:
        ``True`` on success or ``("ERROR", exc)`` on failure.
    """
    if not dates:
        return True
    try:
        cur.execute(f'DELETE FROM "{tab}" WHERE date = ANY(%s)', (dates,))
        return True
    except Exception as e:
        log(f"delete_from_a_table error in {tab}: {e}")
        return "ERROR", e


@logger.with_logging(logs)
@db_decorator()
def duplicated_remover(cur: Any) -> None:
    """Remove redundant middle rows where three consecutive values are equal."""
    cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public'")
    excluded = NON_ACCOUNT_TABLES - {"accounts_balance"}
    table_list = [row[0] for row in cur.fetchall() if row[0] not in excluded]

    for tab in tqdm(table_list, desc="duplicated_remover"):
        cur.execute(f'SELECT date, balance FROM "{tab}" ORDER BY date')
        values = cur.fetchall()
        del_list = [
            values[n + 1][0]
            for n in range(len(values) - 2)
            if values[n][1] == values[n + 1][1] == values[n + 2][1]
        ]
        if del_list:
            cur.execute(f'DELETE FROM "{tab}" WHERE date = ANY(%s)', (del_list,))


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

@logger.with_logging(logs)
@db_decorator()
def KB_main(cur: Any) -> bool | tuple[str, Exception]:
    """Run the full account-balance update pipeline."""
    global db_con

    # ?? 1. Prepare shared variables ??????????????????????????????????????
    log("=== 珥덇린 蹂??以鍮?===")
    try:
        tnow, last_update, exceptions = KB_prepare()
    except Exception as e:
        return "ERROR", e

    # ?? 2. KB open-banking & securities ??????????????????????????????????
    log("=== KB 利앷텒 ?ㅽ뵂諭낇궧/怨꾩쥖 ?낅뜲?댄듃 ===")
    try:
        if os.path.isfile(FILE_KB_PICKLE):
            last_update["KB"] = os.path.getmtime(FILE_KB_PICKLE)
        else:
            raise FileNotFoundError(f"{FILE_KB_PICKLE} is missing")

        log("=== KB.pickle 遺꾩꽍 ===")
        accounts = kbstock_openbank(tnow, exceptions)
        if not isinstance(accounts, dict):
            raise Exception("KB利앷텒-?ㅽ뵂諭낇궧 寃곌낵 媛??먮윭")
        accounts = kbstock(tnow, accounts)
        if not isinstance(accounts, dict):
            raise Exception("KB利앷텒-利앷텒怨꾩쥖 寃곌낵 媛??먮윭")
    except Exception as e:
        return "ERROR", e

    # ?? 3. Manual / CREON entries ?????????????????????????????????????????
    try:
        try:
            with open(FILE_MANUAL, "rb") as f:
                readResult = pickle.load(f)
            for accname, value in readResult.items():
                match accname:
                    case "?⑸퉬":
                        accname = "lab_private"
                        log("=== ?⑸퉬 ?붽퀬 ?낅뜲?댄듃 ===")
                    case "toss利앷텒":
                        accname = "toss"
                        log("=== ?좎벐利앷텒 ?붽퀬 ?낅뜲?댄듃 ===")
                    case _ as accname2 if "@" in accname2:
                        parts   = accname2.split("@")
                        accname = parts[0]
                        label   = "".join(parts[1:])
                        if accname == "?異?":
                            accname = "debt"
                            log(f"=== ?異?({label}) ?낅뜲?댄듃 ===")
                        elif accname == "蹂댄뿕":
                            accname = "insurance"
                            log(f"=== 蹂댄뿕 ({label}) ?낅뜲?댄듃 ===")
                        else:
                            log(f"=== {accname} ({label}) ?낅뜲?댄듃 ===")
                        if accname in accounts:
                            value += accounts[accname]["balance"][tnow]
                accounts[accname] = {"balance": {tnow: value}}
        except Exception as e:
            log(f"manual.pickle 泥섎━ 以??먮윭: {e}")

        log("=== CREON ?붽퀬 ?낅뜲?댄듃 ===")
        if CREON_ACCOUNT_NUMBER in (None, ""):
            raise ValueError("CREON_ACCOUNT_NUMBER is missing in .env")
        accounts = pickle_read(tnow, accounts, fp=FILE_CREON, accname=CREON_ACCOUNT_NUMBER)
        if not isinstance(accounts, dict):
            raise Exception("CREON ?낅뜲?댄듃 ?먮윭")
    except Exception as e:
        return "ERROR", e

    # ?? 4. Sanity-check total  ????????????????????????????????????????????
    try:
        totals = sum(
            acc_data["balance"][max(acc_data["balance"])]
            for acc_data in accounts.values()
        )
        log(f"占?{totals:,}")
        if totals <= 150_000_000:
            raise ValueError("珥앺빀 1??5泥쒕쭔??誘몃쭔 ???곗씠???ㅻ쪟濡??먮떒?섍퀬 醫낅즺?⑸땲??")
    except Exception as e:
        e.add_note("遺꾩꽍 寃곌낵 1??5泥쒕쭔??誘몃쭔 => 臾몄젣 ?덈떎怨??먮떒?섍퀬 醫낅즺")
        return "ERROR", e

    # ?? 5. Card debt  ?????????????????????????????????????????????????????
    go_analysis_card = True
    try:
        if "cards" in last_update:
            cards_excel_mtime = os.path.getmtime(filepath_cardexcel)
            if (
                datetime.date.fromtimestamp(os.path.getmtime("cards.pickle"))
                == datetime.date.today()
                and cards_excel_mtime == last_update["cards"]
            ):
                go_analysis_card = False

        if go_analysis_card:
            accounts = cards_to_accounts(tnow=tnow, accounts=accounts)
            if not isinstance(accounts, dict):
                raise Exception("移대뱶 ?낅뜲?댄듃 ?먮윭")
            last_update["cards"] = os.path.getmtime(filepath_cardexcel)
        else:
            log("移대뱶 excel 蹂???놁쓬")
            last_card(tnow=tnow, accounts=accounts)
    except Exception as e:
        return "ERROR", e

    # ?? 6. Write account info ?????????????????????????????????????????????
    log("=== 怨꾩쥖 ?뺣낫 ?낅뜲?댄듃 ===")
    try:
        info_update(accounts=accounts)
    except Exception as e:
        return "ERROR", e

    # ?? 7. Remove exceptions (build filtered copy ??no in-place mutation) ??
    exc_accounts = set(exceptions.get("accounts", []))
    exc_banks    = set(exceptions.get("bank", []))
    accounts = {
        k: v
        for k, v in accounts.items()
        if k not in exc_accounts
        and v.get("company", "") not in exc_banks
    }

    # ?? 8. Per-account balance update ?????????????????????????????????????
    log("=== 怨꾩쥖 ?붽퀬 ?낅뜲?댄듃 ===")
    balance_update(accounts=accounts, tnow=tnow, exceptions=exceptions)

    # ?? 9. Total balance update ???????????????????????????????????????????
    try:
        if not go_analysis_card:
            log("=== 醫낇빀?붽퀬 ?낅뜲?댄듃 ===")
            print("=== 醫낇빀?붽퀬 ?낅뜲?댄듃 ===", flush=True)
            accounts_balance_update(accounts=accounts)
        else:
            log("=== 醫낇빀?붽퀬 ?꾩껜 媛깆떊 ===")
            print("=== 醫낇빀?붽퀬 ?꾩껜 媛깆떊 ===", flush=True)
            refresh_accounts_balance()
    except Exception as e:
        print(f"[ERROR] 醫낇빀?붽퀬 ?낅뜲?댄듃 ?ㅽ뙣: {e!r}", flush=True)
        return "ERROR", e

    # ?? 10. Daily / monthly diff ??????????????????????????????????????????
    log("=== ?붽퀬 ?쇰퀎/?붾퀎 蹂???낅뜲?댄듃 ===")
    print("=== ?붽퀬 ?쇰퀎/?붾퀎 蹂???낅뜲?댄듃 ===", flush=True)
    try:
        refresh_daydiff()
        refresh_monthdiff()
    except Exception as e:
        print(f"[ERROR] ????diff ?낅뜲?댄듃 ?ㅽ뙣: {e!r}", flush=True)
        return "ERROR", e

    # ?? 11. Deduplicate ???????????????????????????????????????????????????
    log("=== 以묐났?쒓굅 ===")
    print("=== 以묐났?쒓굅 ===", flush=True)
    try:
        duplicated_remover()
    except Exception as e:
        print(f"[ERROR] 以묐났?쒓굅 ?ㅽ뙣: {e!r}", flush=True)
        return "ERROR", e

    # ?? 12. Persist last_update & commit ??????????????????????????????????
    if not lst_udt(operation="save", last_update=last_update):
        raise Exception(".last_update.pickle ????먮윭")

    try:
        cur.close()
        db_con.commit()   # type: ignore[union-attr]
        db_con.close()    # type: ignore[union-attr]
        db_con = None
    except Exception as e:
        log(f"DB ????ㅻ쪟: {e}")

    return True


if __name__ == "__main__":
    result = KB_main()
    if isinstance(result, tuple) and result[0] == "ERROR":
        print(f"\n[ERROR] KB_main ?ㅽ뙣: {result[1]!r}")
        raise SystemExit(1)
    else:
        print("\n=== ?꾨즺 ===")

