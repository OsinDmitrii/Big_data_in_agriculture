from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


AGG_SPECS = {
    "t2m": ["mean", "min", "max"],
    "d2m": ["mean"],
    "tp": ["sum"],
    "swvl1": ["mean"],
    "swvl2": ["mean"],
    "wind_speed_10m": ["mean"],
    # если позже появятся столбцы — просто добавишь сюда:
    # "pev_mm": ["sum"],
    # "evavt_mm": ["sum"],
}


def aggregate_one_month(hourly_path: Path) -> pd.DataFrame:
    df = pd.read_parquet(hourly_path)
    df["ts"] = pd.to_datetime(df["ts"])
    df["day"] = df["ts"].dt.date

    cols_present = [c for c in AGG_SPECS.keys() if c in df.columns]
    agg = {c: AGG_SPECS[c] for c in cols_present}

    g = df.groupby(["region", "day"], as_index=False).agg(agg)

    # расплющиваем multiindex в колонках
    new_cols = []
    for col in g.columns:
        if isinstance(col, tuple):
            base, fn = col
            if fn == "":
                new_cols.append(base)
            else:
                suffix = {"sum": "sum", "mean": "mean", "min": "min", "max": "max"}[fn]
                new_cols.append(f"{base}_{suffix}")
        else:
            new_cols.append(col)
    g.columns = new_cols

    # water_balance если есть tp_sum и pev_sum
    if "tp_sum" in g.columns and "pev_mm_sum" in g.columns:
        g["water_balance"] = g["tp_sum"] - g["pev_mm_sum"]

    return g


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--months", type=str, default="1")
    ap.add_argument("--hourly-root", type=str, default="data/marts/hourly")
    ap.add_argument("--out-root", type=str, default="data/marts/daily")
    args = ap.parse_args()

    months = [int(x) for x in args.months.split(",") if x.strip()]
    hourly_root = Path(args.hourly_root)
    out_root = Path(args.out_root)

    for m in months:
        # собираем все регионы за месяц
        hourly_files = list(hourly_root.glob(f"region=*/year={args.year}/month={m:02d}.parquet"))
        if not hourly_files:
            print("SKIP month (no hourly parquet):", m)
            continue

        daily_parts = []
        for hp in hourly_files:
            daily_parts.append(aggregate_one_month(hp))

        daily_df = pd.concat(daily_parts, ignore_index=True)

        # сохраняем один файл на месяц (все регионы внутри)
        out_dir = out_root / f"year={args.year}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"month={m:02d}.parquet"
        daily_df.to_parquet(out_file, index=False)
        print("OK:", out_file)


if __name__ == "__main__":
    main()
