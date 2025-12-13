# flows/download_era5_land.py
from __future__ import annotations

import argparse
import calendar
import json
import os
from pathlib import Path

import cdsapi
import yaml
from prefect import flow, task, get_run_logger

TIMES = [f"{h:02d}:00" for h in range(24)]

# можно передавать короткие коды, а в CDS уйдут корректные имена переменных
VAR_MAP = {
    "t2m": "2m_temperature",
    "d2m": "2m_dewpoint_temperature",
    "tp": "total_precipitation",
    "u10": "10m_u_component_of_wind",
    "v10": "10m_v_component_of_wind",
    "swvl1": "volumetric_soil_water_layer_1",
    "swvl2": "volumetric_soil_water_layer_2",
    "ssrd": "surface_solar_radiation_downwards",
    "ssr": "surface_net_solar_radiation",
    "evavt": "evaporation_from_vegetation_transpiration",
    "pev": "potential_evaporation",
    "lai_hv": "leaf_area_index_high_vegetation",
    "lai_lv": "leaf_area_index_low_vegetation",
}


def _project_root() -> Path:
    # flows/ лежит в корне проекта
    return Path(__file__).resolve().parents[1]


def _resolve(p: str) -> Path:
    pp = Path(p)
    return pp if pp.is_absolute() else (_project_root() / pp)


def _month_days(year: int, month: int, limit_days: int | None = None) -> list[str]:
    n = calendar.monthrange(year, month)[1]
    days = [f"{d:02d}" for d in range(1, n + 1)]
    return days[:limit_days] if limit_days else days


def _normalize_variables(vars_in: list[str]) -> list[str]:
    out = []
    for v in vars_in:
        v = v.strip()
        if not v:
            continue
        out.append(VAR_MAP.get(v, v))  # если уже длинное имя — останется как есть
    return out


@task(retries=2, retry_delay_seconds=30)
def download_month(
    *,
    dataset: str,
    region: str,
    area: list[float],
    year: int,
    month: int,
    variables: list[str],
    out_root: str,
    limit_days: int | None,
) -> str:
    logger = get_run_logger()

    out_dir = _resolve(out_root) / f"region={region}" / f"year={year}"
    out_dir.mkdir(parents=True, exist_ok=True)

    target = out_dir / f"month={month:02d}.nc"
    meta = out_dir / f"month={month:02d}.request.json"
    tmp = out_dir / f"month={month:02d}.nc.part"

    if target.exists():
        logger.info(f"SKIP {target}")
        return str(target)

    req = {
        "product_type": "reanalysis",
        "format": "netcdf",
        "variable": variables,
        "year": str(year),
        "month": f"{month:02d}",
        "day": _month_days(year, month, limit_days=limit_days),
        "time": TIMES,
        "area": area,  # [north, west, south, east]
    }
    meta.write_text(json.dumps(req, ensure_ascii=False, indent=2), encoding="utf-8")

    if tmp.exists():
        tmp.unlink()

    logger.info(f"DOWNLOADING region={region} year={year} month={month:02d}")
    c = cdsapi.Client()
    c.retrieve(dataset, req, str(tmp))
    os.replace(tmp, target)

    logger.info(f"OK {target}")
    return str(target)


@flow(name="download-era5-land")
def download_era5_land(
    *,
    year: int,
    months: list[int] | None = None,
    variables: list[str] | None = None,
    limit_days: int | None = 7,
    only_regions: list[str] | None = None,
    regions_yaml: str = "config/regions.yaml",
    out_root: str = "data/raw/era5-land",
    dataset: str = "reanalysis-era5-land",
) -> list[str]:
    logger = get_run_logger()

    if months is None:
        months = [1]

    if variables is None:
        # базовый набор
        variables = ["t2m", "d2m", "tp", "u10", "v10", "swvl1", "swvl2"]

    variables = _normalize_variables(variables)

    regions_path = _resolve(regions_yaml)
    cfg = yaml.safe_load(regions_path.read_text(encoding="utf-8"))

    regions = only_regions if only_regions else list(cfg.keys())

    outputs: list[str] = []
    for r in regions:
        if r not in cfg:
            logger.warning(f"Region '{r}' not found in {regions_path}")
            continue
        area = cfg[r].get("area")
        if not area or area == [0.0, 0.0, 0.0, 0.0]:
            logger.warning(f"Region '{r}' has empty area, skipping")
            continue

        for m in months:
            outputs.append(
                download_month(
                    dataset=dataset,
                    region=r,
                    area=area,
                    year=year,
                    month=m,
                    variables=variables,
                    out_root=out_root,
                    limit_days=limit_days,
                )
            )

    return outputs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--months", type=str, default="1", help="например: 1,2,3")
    ap.add_argument("--limit-days", type=int, default=7, help="первые N дней месяца (для базового объёма)")
    ap.add_argument("--regions", type=str, default="", help="например: bashkortostan,belarus,... (пусто = все из yaml)")
    ap.add_argument("--vars", type=str, default="", help="например: t2m,d2m,tp,u10,v10,swvl1,swvl2")
    ap.add_argument("--regions-yaml", type=str, default="config/regions.yaml")
    ap.add_argument("--out-root", type=str, default="data/raw/era5-land")
    ap.add_argument("--dataset", type=str, default="reanalysis-era5-land")
    args = ap.parse_args()

    months = [int(x) for x in args.months.split(",") if x.strip()]
    regions = [x.strip() for x in args.regions.split(",") if x.strip()] or None
    variables = [x.strip() for x in args.vars.split(",") if x.strip()] or None

    download_era5_land(
        year=args.year,
        months=months,
        variables=variables,
        limit_days=args.limit_days,
        only_regions=regions,
        regions_yaml=args.regions_yaml,
        out_root=args.out_root,
        dataset=args.dataset,
    )


if __name__ == "__main__":
    main()
