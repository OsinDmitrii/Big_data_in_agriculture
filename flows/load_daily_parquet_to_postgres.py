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


def upsert_df(conn, df: pd.DataFrame, table: str = "marts.era5_daily"):
    df = df.copy()

    # ожидаем: region, day, ...
    if "day" not in df.columns:
        raise ValueError("No 'day' column in dataframe")

    # day -> date
    df["day"] = pd.to_datetime(df["day"]).dt.date

    cols = list(df.columns)
    if "region" not in cols or "day" not in cols:
        raise ValueError("Expected columns: region, day")

    set_cols = [c for c in cols if c not in ("region", "day")]
    set_sql = ", ".join([f"{c}=EXCLUDED.{c}" for c in set_cols])

    sql = f"""
        INSERT INTO {table} ({",".join(cols)})
        VALUES %s
        ON CONFLICT (region, day) DO UPDATE SET {set_sql};
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
    ap.add_argument("--daily-root", type=str, default="data/marts/daily")
    ap.add_argument("--table", type=str, default="marts.era5_daily")
    args = ap.parse_args()

    months = [int(x) for x in args.months.split(",") if x.strip()]
    daily_root = Path(args.daily_root)

    files = [daily_root / f"year={args.year}" / f"month={m:02d}.parquet" for m in months]
    files = [f for f in files if f.exists()]

    if not files:
        print("No daily parquet files found.")
        return

    conn = connect()
    try:
        for fp in files:
            df = pd.read_parquet(fp)
            upsert_df(conn, df, table=args.table)
            print("OK:", fp)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
