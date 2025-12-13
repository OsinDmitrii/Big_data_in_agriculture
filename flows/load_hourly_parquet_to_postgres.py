from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


def connect():
    host = os.getenv("PGHOST", "127.0.0.1")
    port = int(os.getenv("PGPORT", "5432"))
    db = os.getenv("PGDATABASE", "agri")
    user = os.getenv("PGUSER", "agri")
    pwd = os.getenv("PGPASSWORD", "agri")
    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pwd)


def upsert_df(conn, df: pd.DataFrame, table: str = "marts.era5_hourly"):
    # ожидаем: region, ts, ...
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"])

    cols = list(df.columns)
    assert "region" in cols and "ts" in cols

    # ON CONFLICT update для всех остальных колонок
    set_cols = [c for c in cols if c not in ("region", "ts")]
    set_sql = ", ".join([f"{c}=EXCLUDED.{c}" for c in set_cols])

    sql = f"""
        INSERT INTO {table} ({",".join(cols)})
        VALUES %s
        ON CONFLICT (region, ts) DO UPDATE SET {set_sql};
    """

    values = [tuple(x) for x in df.itertuples(index=False, name=None)]
    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=5000)
    conn.commit()


def main():
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--months", type=str, default="1")
    ap.add_argument("--hourly-root", type=str, default="data/marts/hourly")
    ap.add_argument("--table", type=str, default="marts.era5_hourly")
    args = ap.parse_args()

    months = [int(x) for x in args.months.split(",") if x.strip()]
    hourly_root = Path(args.hourly_root)

    files = []
    for m in months:
        files += list(hourly_root.glob(f"region=*/year={args.year}/month={m:02d}.parquet"))

    if not files:
        print("No parquet files found.")
        return

    conn = connect()
    try:
        for fp in sorted(files):
            df = pd.read_parquet(fp)
            upsert_df(conn, df, table=args.table)
            print("OK:", fp)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
