# dask_jobs/extract_era5.py
from __future__ import annotations

import argparse
import calendar
import json
from pathlib import Path

import cdsapi
import yaml


DEFAULT_VARS = [
    "t2m", "d2m", "tp", "u10", "v10",
    "swvl1", "swvl2",
    # добавишь потом: "ssrd","ssr","pev","evavt","lai_hv","lai_lv" и т.д.
]


def days_in_month(year: int, month: int) -> list[str]:
    n = calendar.monthrange(year, month)[1]
    return [f"{d:02d}" for d in range(1, n + 1)]


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--months", type=str, default="1")
    ap.add_argument("--regions-yaml", type=str, default="config/regions.yaml")
    ap.add_argument("--raw-root", type=str, default="data/raw/era5-land")
    ap.add_argument("--vars", type=str, default=",".join(DEFAULT_VARS))
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    months = [int(x) for x in args.months.split(",") if x.strip()]
    variables = [v.strip() for v in args.vars.split(",") if v.strip()]

    cfg = yaml.safe_load(Path(args.regions_yaml).read_text(encoding="utf-8"))
    regions = [r for r in cfg.keys() if cfg[r]["area"] != [0.0, 0.0, 0.0, 0.0]]

    raw_root = Path(args.raw_root)
    c = cdsapi.Client()

    for region in regions:
        area = cfg[region]["area"]  # [N, W, S, E]
        for m in months:
            out_dir = raw_root / f"region={region}" / f"year={args.year}"
            ensure_dir(out_dir)

            out_zip = out_dir / f"month={m:02d}.zip"
            req_json = out_dir / f"month={m:02d}.request.json"

            if out_zip.exists() and not args.force:
                print("SKIP (exists):", out_zip)
                continue

            request = {
                "variable": variables,
                "year": str(args.year),
                "month": f"{m:02d}",
                "day": days_in_month(args.year, m),
                "time": [f"{h:02d}:00" for h in range(24)],
                "area": area,
                "format": "netcdf",
            }

            req_json.write_text(json.dumps(request, ensure_ascii=False, indent=2), encoding="utf-8")
            print("DOWNLOAD:", region, args.year, f"{m:02d}", "->", out_zip)

            c.retrieve("reanalysis-era5-land", request, str(out_zip))

    print("DONE")


if __name__ == "__main__":
    main()
