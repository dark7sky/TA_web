from __future__ import annotations

import argparse
import datetime as dt
import os
from typing import Any

from dotenv import load_dotenv

from normalized_pg import get_db_connection

load_dotenv()


def required_env(key: str) -> str:
    value = os.getenv(key)
    if value in (None, ""):
        raise RuntimeError(f"Required env var missing: {key}")
    return value


def open_influxdb() -> tuple[Any, Any, str, str]:
    from influxdb_client import InfluxDBClient
    from influxdb_client.client.write_api import SYNCHRONOUS

    url = required_env("INFLUX_URL")
    token = required_env("INFLUX_TOKEN")
    org = required_env("INFLUX_ORG")
    bucket = required_env("INFLUX_BUCKET")
    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    return client, write_api, org, bucket


def iter_history_rows(limit: int | None = None):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        sql = """
            SELECT account_key, recorded_at, balance
            FROM account_balance_history
            ORDER BY account_key, recorded_at
        """
        params = tuple()
        if limit is not None:
            sql += " LIMIT %s"
            params = (limit,)
        cur.execute(sql, params)
        for row in cur.fetchall():
            yield row
    finally:
        conn.close()


def write_history(limit: int | None = None) -> None:
    from influxdb_client import Point, WritePrecision

    client, write_api, org, bucket = open_influxdb()
    count = 0
    try:
        for account_key, recorded_at, balance in iter_history_rows(limit=limit):
            timestamp = recorded_at
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=dt.timezone.utc)
            point = (
                Point("accounts")
                .tag("account", str(account_key))
                .field("balance", int(balance))
                .time(timestamp.astimezone(dt.timezone.utc), WritePrecision.NS)
            )
            write_api.write(bucket, org, point)
            count += 1
            if count % 500 == 0:
                print(f"[WRITE] {count} rows", flush=True)
        print(f"[OK] wrote {count} rows to InfluxDB")
    finally:
        client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export normalized PostgreSQL account history to InfluxDB.")
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit for smoke testing.")
    args = parser.parse_args()
    write_history(limit=args.limit)
