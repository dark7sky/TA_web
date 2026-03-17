from __future__ import annotations

import datetime as dt
import glob
import os
import pickle
from typing import Any

from normalized_pg import (
    fetch_account_history,
    get_db_connection,
    insert_account_history,
    is_special_account_key,
    parse_legacy_timestamp,
    rebuild_portfolio_summaries,
    upsert_accounts,
    ensure_normalized_schema,
)

FILE_LAST_UPDATE = ".last_update.pickle"
FILE_LAST_UPDATE_BK = ".last_update_bk.pickle"
FILE_KB_PICKLE = "KB.pickle"
FILE_KBSELF_PICKLE = "KBself.pickle"
FILE_KBSELF2_PICKLE = "KBself2.pickle"
FILE_CARD_EXCEL = "cards.xlsx"


def parse_int(raw_value: Any, default: int = 0) -> int:
    if raw_value is None:
        return default
    if isinstance(raw_value, bool):
        return int(raw_value)
    if isinstance(raw_value, int):
        return raw_value
    if isinstance(raw_value, float):
        return int(raw_value)

    text = str(raw_value).strip().replace(",", "")
    if not text:
        return default
    try:
        return int(text)
    except ValueError:
        try:
            return int(float(text))
        except ValueError:
            return default


def load_pickle_value(filepath: str, default: Any = 0) -> Any:
    try:
        with open(filepath, "rb") as file_obj:
            return pickle.load(file_obj)
    except FileNotFoundError:
        return default


def lst_udt(operation: str = "read", last_update: dict[str, float] | None = None) -> dict[str, float] | bool:
    last_update = last_update or {}
    if operation == "read":
        try:
            with open(FILE_LAST_UPDATE, "rb") as file_obj:
                return pickle.load(file_obj)
        except FileNotFoundError:
            return {"KB": 0.0}
        except pickle.UnpicklingError:
            with open(FILE_LAST_UPDATE_BK, "rb") as file_obj:
                return pickle.load(file_obj)

    if operation == "save":
        try:
            with open(FILE_LAST_UPDATE, "wb") as file_obj:
                pickle.dump(last_update, file_obj)
            return True
        except Exception:
            return False

    return False


def kbstock(tnow: dt.datetime, exceptions: dict[str, tuple[str, ...] | list[str]]) -> dict[str, dict[str, Any]]:
    rows = load_pickle_value(FILE_KB_PICKLE, default=[])
    if not isinstance(rows, list):
        raise TypeError(f"{FILE_KB_PICKLE} must contain a list")

    accounts: dict[str, dict[str, Any]] = {}
    skip_banks = set(exceptions.get("bank", []))
    skip_accounts = set(exceptions.get("accounts", []))

    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 5:
            continue
        company = str(row[0]).strip()
        account_type = str(row[1]).strip()
        account_key = str(row[2]).strip()
        name = str(row[3]).strip() or account_key
        balance = parse_int(row[4], default=0)
        if not account_key or company in skip_banks or account_key in skip_accounts:
            continue
        accounts[account_key] = {
            "company": company,
            "type": account_type,
            "name": name,
            "memo": "",
            "balance": {tnow: balance},
        }

    kbself = load_pickle_value(FILE_KBSELF_PICKLE, default=0)
    kbself2 = load_pickle_value(FILE_KBSELF2_PICKLE, default=0)

    accounts.setdefault(
        "36598545801",
        {
            "company": "KB Securities",
            "type": "Brokerage",
            "name": "KB Securities",
            "memo": "",
            "balance": {},
        },
    )
    accounts["36598545801"]["balance"][tnow] = parse_int(kbself, default=0)

    accounts.setdefault(
        "37791800001",
        {
            "company": "KB Securities",
            "type": "Brokerage",
            "name": "KB Stock",
            "memo": "",
            "balance": {},
        },
    )
    accounts["37791800001"]["balance"][tnow] = parse_int(kbself2, default=0)
    return accounts


class account_db:
    def __init__(self, accounts: dict[str, dict[str, Any]], fp: str = "stored.db"):
        self.fp = fp
        self.accounts = accounts
        self.con = get_db_connection()
        self.cur = self.con.cursor()
        ensure_normalized_schema(self.cur)

    def _metadata_rows(self, accounts: dict[str, dict[str, Any]] | None = None) -> list[tuple[Any, ...]]:
        rows: list[tuple[Any, ...]] = []
        source_accounts = accounts if accounts is not None else self.accounts
        for account_key, payload in source_accounts.items():
            rows.append(
                (
                    account_key,
                    str(payload.get("company", "")),
                    str(payload.get("type", payload.get("account_type", ""))),
                    str(payload.get("name", account_key)) or account_key,
                    str(payload.get("memo", "")),
                    is_special_account_key(account_key),
                    True,
                )
            )
        return rows

    def _history_rows(self, accounts: dict[str, dict[str, Any]] | None = None) -> list[tuple[Any, ...]]:
        rows: list[tuple[Any, ...]] = []
        source_accounts = accounts if accounts is not None else self.accounts
        for account_key, payload in source_accounts.items():
            balances = payload.get("balance", {})
            if not isinstance(balances, dict):
                continue
            source = str(payload.get("source", "legacy_ysfunc"))
            for recorded_at, balance in balances.items():
                rows.append(
                    (
                        account_key,
                        parse_legacy_timestamp(recorded_at),
                        parse_int(balance, default=0),
                        source,
                    )
                )
        return rows

    def _record_single_snapshot(
        self,
        account_key: str,
        balance: int,
        recorded_at: dt.datetime,
        source: str,
        company: str = "",
        account_type: str = "",
        name: str = "",
        memo: str = "",
    ) -> None:
        payload = {
            account_key: {
                "company": company or account_key,
                "type": account_type or ("Special" if is_special_account_key(account_key) else ""),
                "name": name or account_key,
                "memo": memo,
                "source": source,
                "balance": {parse_legacy_timestamp(recorded_at): parse_int(balance, default=0)},
            }
        }
        upsert_accounts(self.cur, self._metadata_rows(payload))
        insert_account_history(self.cur, self._history_rows(payload))
        self.accounts[account_key] = payload[account_key]

    def info_update(self) -> None:
        upsert_accounts(self.cur, self._metadata_rows())

    def balance_update(self) -> None:
        self.info_update()
        insert_account_history(self.cur, self._history_rows())

    def toss(self, tnow: dt.datetime, fp: str = "toss.pickle") -> None:
        balance = parse_int(load_pickle_value(fp, default=0), default=0)
        self._record_single_snapshot("toss", balance, tnow, "legacy_toss")

    def lab_private(self, tnow: dt.datetime, fp: str = "lab_private.pickle") -> None:
        balance = parse_int(load_pickle_value(fp, default=0), default=0)
        self._record_single_snapshot("lab_private", balance, tnow, "legacy_lab_private")

    def coins(self, tnow: dt.datetime, fp: str = "BTH.pickle") -> None:
        balance = parse_int(load_pickle_value(fp, default=0), default=0)
        self._record_single_snapshot("bithumb", balance, tnow, "legacy_bithumb")

    def cards_to_accounts(self, tnow: dt.datetime, filepath_exel: str | None = None) -> dict[str, dict[str, Any]]:
        import cards

        excel_path = filepath_exel
        if not excel_path:
            if os.path.exists(FILE_CARD_EXCEL):
                excel_path = FILE_CARD_EXCEL
            else:
                matches = sorted(glob.glob("*.xlsx"))
                excel_path = matches[0] if matches else None

        if not excel_path or not os.path.exists(excel_path):
            print("cards_to_accounts skipped: no Excel file found")
            return self.accounts

        cards.main(self.cur, filepath_exel=excel_path)
        latest_rows = fetch_account_history(self.cur, "accounts_cards", limit=1)
        card_balance = latest_rows[0][1] if latest_rows else 0
        self._record_single_snapshot(
            "accounts_cards",
            card_balance,
            tnow,
            "cards",
            company="accounts_cards",
            account_type="Special",
            name="accounts_cards",
        )
        return self.accounts

    def accounts_balance_update(self) -> None:
        rebuild_portfolio_summaries(self.cur)

    def duplicated_remover(self) -> None:
        print("duplicated_remover skipped: normalized schema uses primary keys for exact dedupe")

    def refresh_daydiff(self) -> None:
        rebuild_portfolio_summaries(self.cur)

    def refresh_monthdiff(self) -> None:
        rebuild_portfolio_summaries(self.cur)

    def remove_milliseconds(self) -> None:
        self.cur.execute(
            """
            SELECT COUNT(*)
            FROM account_balance_history
            WHERE recorded_at <> date_trunc('second', recorded_at)
            """
        )
        count = int(self.cur.fetchone()[0])
        print(f"remove_milliseconds skipped: found {count} sub-second rows")

    def refresh_accounts_balance(self) -> None:
        rebuild_portfolio_summaries(self.cur)

    def close(self) -> None:
        try:
            self.cur.close()
        finally:
            self.con.close()


def delete_from_a_table(cur: Any, tab: str, date: str) -> bool:
    try:
        if tab == "accounts_balance":
            cur.execute("DELETE FROM portfolio_balance_history WHERE recorded_at = %s", (parse_legacy_timestamp(date),))
        elif tab == "accounts_daydiff":
            cur.execute("DELETE FROM portfolio_daydiff WHERE balance_date = %s", (parse_legacy_timestamp(date).date(),))
        elif tab == "accounts_monthdiff":
            cur.execute("DELETE FROM portfolio_monthdiff WHERE balance_date = %s", (parse_legacy_timestamp(date).date(),))
        elif tab == "accounts_diff":
            return False
        else:
            cur.execute(
                "DELETE FROM account_balance_history WHERE account_key = %s AND recorded_at = %s",
                (tab, parse_legacy_timestamp(date)),
            )
        return True
    except Exception as exc:
        print(f"delete_from_a_table failed for {tab} / {date}: {exc}")
        return False


def delete_from_all_table(db: account_db, date: str) -> None:
    target_ts = parse_legacy_timestamp(date)
    db.cur.execute("DELETE FROM account_balance_history WHERE recorded_at = %s", (target_ts,))
    rebuild_portfolio_summaries(db.cur)


if __name__ == "__main__":
    database = account_db({}, "stored.db")
    try:
        print("========== Normalized maintenance ==========")
        database.remove_milliseconds()
        database.duplicated_remover()
        print("========== Refresh summaries ==========")
        database.refresh_accounts_balance()
        database.con.commit()
    finally:
        database.close()
