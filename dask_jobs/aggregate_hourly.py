from __future__ import annotations

import argparse
import tempfile
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
import yaml
from dask.distributed import Client


def convert_units(df: pd.DataFrame) -> pd.DataFrame:
    # t2m, d2m: K -> C
    for col in ["t2m", "d2m"]:
        if col in df.columns:
            df[col] = df[col] - 273.15

    # tp: meters -> mm
    if "tp" in df.columns:
        df["tp"] = df["tp"] * 1000.0

    # wind speed from u10, v10
    if "u10" in df.columns and "v10" in df.columns:
        df["wind_speed_10m"] = np.sqrt(df["u10"] ** 2 + df["v10"] ** 2)

    return df


def _extract_first_nc(zip_path: Path, td: Path) -> Path:
    with zipfile.ZipFile(zip_path, "r") as zf:
        nc_names = [n for n in zf.namelist() if n.lower().endswith(".nc")]
        if not nc_names:
            raise RuntimeError(f"ZIP без .nc внутри: {zip_path}")
        name = nc_names[0]
        zf.extract(name, td)
        extracted = td / name
        # если был путь с подпапками — переносим в корень temp
        out_nc = td / Path(name).name
        if extracted != out_nc:
            out_nc.parent.mkdir(parents=True, exist_ok=True)
            extracted.replace(out_nc)
        return out_nc


def region_mean_timeseries(path_str: str, variables: list[str]) -> pd.DataFrame:
    path = Path(path_str)

    with tempfile.TemporaryDirectory() as td0:
        td = Path(td0)

        # если "month=01.nc" на самом деле ZIP — ок
        if zipfile.is_zipfile(path):
            nc_path = _extract_first_nc(path, td)
        else:
            nc_path = path

        ds = xr.open_dataset(nc_path, engine=None)

        try:
            vars_present = [v for v in variables if v in ds.data_vars]
            if not vars_present:
                raise RuntimeError(f"Нет нужных переменных в файле: {path}. Есть: {list(ds.data_vars)}")

            ds = ds[vars_present]

            # mean по lat/lon
            if "latitude" in ds.dims and "longitude" in ds.dims:
                agg = ds.mean(dim=["latitude", "longitude"], skipna=True)
            else:
                dims = [d for d in ds.dims if d.lower() in ("lat", "lon", "latitude", "longitude")]
                if not dims:
                    raise RuntimeError(f"Не нашёл lat/lon dims в {path}. Dims={list(ds.dims)}")
                agg = ds.mean(dim=dims, skipna=True)

            df = agg.to_dataframe().reset_index()

            # время
            if "valid_time" in df.columns:
                df = df.rename(columns={"valid_time": "ts"})
            elif "time" in df.columns:
                df = df.rename(columns={"time": "ts"})
            else:
                raise RuntimeError(f"Не нашёл time/valid_time в {path}. Cols={list(df.columns)}")

            keep = ["ts"] + vars_present
            df = df[keep].sort_values("ts").reset_index(drop=True)

            return convert_units(df)
        finally:
            ds.close()


def process_one(
    region: str,
    year: int,
    month: int,
    raw_root: str,
    out_root: str,
    variables: list[str],
) -> str:
    raw_root_p = Path(raw_root)
    out_root_p = Path(out_root)

    p1 = raw_root_p / f"region={region}" / f"year={year}" / f"month={month:02d}.nc"
    p2 = raw_root_p / f"region={region}" / f"year={year}" / f"month={month:02d}.zip"

    if p1.exists():
        inp = p1
    elif p2.exists():
        inp = p2
    else:
        return f"SKIP (no raw): {p1}"

    df = region_mean_timeseries(str(inp), variables)
    df.insert(0, "region", region)

    out_dir = out_root_p / f"region={region}" / f"year={year}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"month={month:02d}.parquet"
    df.to_parquet(out_file, index=False)

    return f"OK: {out_file}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--months", type=str, default="1")
    ap.add_argument("--regions-yaml", type=str, default="config/regions.yaml")
    ap.add_argument("--raw-root", type=str, default="data/raw/era5-land")
    ap.add_argument("--out-root", type=str, default="data/marts/hourly")
    ap.add_argument("--vars", type=str, default="t2m,d2m,tp,u10,v10,swvl1,swvl2")
    ap.add_argument("--dask", type=str, default="")
    args = ap.parse_args()

    months = [int(x) for x in args.months.split(",") if x.strip()]
    variables = [v.strip() for v in args.vars.split(",") if v.strip()]

    cfg = yaml.safe_load(Path(args.regions_yaml).read_text(encoding="utf-8"))
    regions = [r for r in cfg.keys() if cfg[r]["area"] != [0.0, 0.0, 0.0, 0.0]]

    if args.dask.strip():
        client = Client(args.dask.strip())
        futures = []
        for region in regions:
            for m in months:
                futures.append(
                    client.submit(
                        process_one,
                        region,
                        args.year,
                        m,
                        args.raw_root,
                        args.out_root,
                        variables,
                        pure=False,
                    )
                )
        for fut in futures:
            print(fut.result())
        client.close()
    else:
        for region in regions:
            for m in months:
                print(process_one(region, args.year, m, args.raw_root, args.out_root, variables))


if __name__ == "__main__":
    main()
