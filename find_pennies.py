from __future__ import annotations

from normalized_pg import fetch_accounts_with_latest, get_db_connection


def prompt_int(label: str) -> int:
    while True:
        raw = input(label).strip().replace(",", "")
        if raw.lstrip("-").isdigit():
            return int(raw)
        print("Please enter an integer amount.")


def load_latest_balances() -> dict[str, tuple[int, tuple[str, str, str, str]]]:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        rows = fetch_accounts_with_latest(cur)
        return {
            row.account_key: (
                row.latest_balance,
                (row.company, row.account_type, row.name, row.memo),
            )
            for row in rows
        }
    finally:
        conn.close()


if __name__ == "__main__":
    result = load_latest_balances()

    while True:
        bottom = prompt_int("Amount bottom limit: ")
        top = prompt_int("Amount top limit: ")
        if bottom > top:
            bottom, top = top, bottom

        matches = [
            (account_key, payload)
            for account_key, payload in result.items()
            if bottom <= int(payload[0]) <= top
        ]
        matches.sort(key=lambda item: (item[1][0], item[0]))

        for account_key, payload in matches:
            print(f"{account_key}: {payload}")
        print(f"Total matched accounts: {len(matches)}")
