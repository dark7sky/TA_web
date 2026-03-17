from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd

from normalized_pg import fetch_accounts_with_latest, get_app_timezone, get_db_connection, is_numeric_account_key


def load_numeric_account_history() -> tuple[pd.DataFrame, list[str]]:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        numeric_accounts = [
            row.account_key
            for row in fetch_accounts_with_latest(cur, include_inactive=False)
            if is_numeric_account_key(row.account_key)
        ]
        if not numeric_accounts:
            return pd.DataFrame(columns=["account_key", "recorded_at", "balance"]), []

        cur.execute(
            """
            SELECT account_key, recorded_at, balance
            FROM account_balance_history
            WHERE account_key = ANY(%s)
            ORDER BY account_key, recorded_at
            """,
            (numeric_accounts,),
        )
        dataframe = pd.DataFrame(cur.fetchall(), columns=["account_key", "recorded_at", "balance"])
        return dataframe, numeric_accounts
    finally:
        conn.close()


if __name__ == "__main__":
    dataframe, numeric_tables = load_numeric_account_history()
    if dataframe.empty:
        print("No numeric account history found in normalized PostgreSQL.")
        raise SystemExit(0)

    timezone = get_app_timezone()
    dataframe["recorded_at"] = pd.to_datetime(dataframe["recorded_at"], utc=True).dt.tz_convert(timezone)
    dataframe["balance"] = pd.to_numeric(dataframe["balance"], errors="coerce")
    dataframe.dropna(subset=["recorded_at", "balance"], inplace=True)

    print(f"Found {len(numeric_tables)} numeric accounts")

    fig, ax = plt.subplots(figsize=(15, 7))
    for account_key in numeric_tables:
        subset = dataframe[dataframe["account_key"] == account_key]
        if subset.empty:
            continue
        ax.plot(subset["recorded_at"], subset["balance"], label=account_key, marker="o", markersize=2)

    ax.set_title("Numeric account balance history")
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("Balance")
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.legend(title="Account", ncol=2, fontsize=8)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
