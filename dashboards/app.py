# dashboards/app.py
from __future__ import annotations

import os
from datetime import datetime, date

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text


st.set_page_config(page_title="ERA5-Land Dashboard", layout="wide")


def pg_url() -> str:
    host = os.getenv("PGHOST", "127.0.0.1")
    port = os.getenv("PGPORT", "5433")  # наружу у тебя postgres на 5433
    db = os.getenv("PGDATABASE", "agri")
    user = os.getenv("PGUSER", "agri")
    pwd = os.getenv("PGPASSWORD", "agri")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


@st.cache_data(ttl=60)
def load_regions() -> list[str]:
    eng = create_engine(pg_url())
    q = text("select distinct region from marts.era5_daily order by 1;")
    with eng.connect() as c:
        return [r[0] for r in c.execute(q).fetchall()]


@st.cache_data(ttl=60)
def load_daily(regions: list[str], start: date, end: date) -> pd.DataFrame:
    eng = create_engine(pg_url())
    q = text("""
        select *
        from marts.era5_daily
        where region = any(:regions)
          and day between :start and :end
        order by region, day;
    """)
    with eng.connect() as c:
        df = pd.read_sql(q, c, params={"regions": regions, "start": start, "end": end})
    df["day"] = pd.to_datetime(df["day"])
    return df


@st.cache_data(ttl=60)
def load_hourly(regions: list[str], start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    eng = create_engine(pg_url())
    q = text("""
        select *
        from marts.era5_hourly
        where region = any(:regions)
          and ts between :start_dt and :end_dt
        order by region, ts;
    """)
    with eng.connect() as c:
        df = pd.read_sql(q, c, params={"regions": regions, "start_dt": start_dt, "end_dt": end_dt})
    df["ts"] = pd.to_datetime(df["ts"])
    return df


def wide_series(df: pd.DataFrame, time_col: str, metric: str) -> pd.DataFrame:
    if metric not in df.columns:
        return pd.DataFrame()
    return (
        df.pivot_table(index=time_col, columns="region", values=metric, aggfunc="mean")
        .sort_index()
    )


def kpi_row(df: pd.DataFrame, time_col: str):
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Строк", f"{len(df):,}".replace(",", " "))
    c2.metric("Регионов", str(df["region"].nunique()))
    c3.metric("Начало", str(df[time_col].min()))
    c4.metric("Конец", str(df[time_col].max()))


st.title("ERA5-Land • 4 региона • витрины в Postgres")

with st.sidebar:
    st.subheader("Подключение / выбор")
    try:
        regions_all = load_regions()
        st.success("Postgres: OK")
    except Exception as e:
        st.error("Postgres: нет подключения")
        st.code(str(e))
        st.stop()

    regions = st.multiselect("Регионы", regions_all, default=regions_all[:1] if regions_all else [])
    if not regions:
        st.stop()

    # дефолт под твои текущие данные (7 дней)
    default_start = date(2022, 1, 1)
    default_end = date(2022, 1, 7)
    d1, d2 = st.date_input("Период (включительно)", value=(default_start, default_end))

    st.caption("Берётся из env: PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD")


tab_daily, tab_hourly = st.tabs(["Daily (дни)", "Hourly (часы)"])

# -------------------- DAILY --------------------
with tab_daily:
    st.subheader("Daily витрина (marts.era5_daily)")

    daily_metrics = [
        "t2m_mean", "t2m_min", "t2m_max",
        "d2m_mean",
        "tp_sum",
        "swvl1_mean", "swvl2_mean",
        "wind_speed_10m_mean",
    ]

    left, right = st.columns([2, 1])
    with left:
        metric = st.selectbox("Метрика для сравнения регионов", daily_metrics, index=daily_metrics.index("t2m_mean"))
    with right:
        show_table = st.checkbox("Показать таблицу", value=False)

    df = load_daily(regions, d1, d2)
    if df.empty:
        st.warning("Нет данных daily за выбранный период.")
    else:
        kpi_row(df, "day")

        st.divider()
        series = wide_series(df, "day", metric)
        st.line_chart(series)

        st.divider()
        # Осадки — столбиками (если выбраны)
        if "tp_sum" in df.columns:
            st.subheader("Осадки (tp_sum) — столбиками")
            tp = wide_series(df, "day", "tp_sum")
            # st.bar_chart умеет рисовать wide dataframe
            st.bar_chart(tp)

        if show_table:
            st.subheader("Raw daily (из БД)")
            st.dataframe(df, width="stretch")

# -------------------- HOURLY --------------------
with tab_hourly:
    st.subheader("Hourly витрина (marts.era5_hourly)")

    hourly_metrics = [
        "t2m", "d2m",
        "tp",
        "swvl1", "swvl2",
        "wind_speed_10m",
        "u10", "v10",
    ]

    left, right = st.columns([2, 1])
    with left:
        metric_h = st.selectbox("Метрика для сравнения регионов", hourly_metrics, index=hourly_metrics.index("t2m"))
    with right:
        show_table_h = st.checkbox("Показать таблицу (hourly)", value=False)

    start_dt = datetime.combine(d1, datetime.min.time())
    end_dt = datetime.combine(d2, datetime.max.time())

    dfh = load_hourly(regions, start_dt, end_dt)
    if dfh.empty:
        st.warning("Нет данных hourly за выбранный период.")
    else:
        kpi_row(dfh, "ts")

        st.divider()
        series_h = wide_series(dfh, "ts", metric_h)
        st.line_chart(series_h)

        st.divider()
        # Осадки hourly — столбиками
        if "tp" in dfh.columns:
            st.subheader("Осадки (tp) — столбиками")
            tph = wide_series(dfh, "ts", "tp")
            st.bar_chart(tph)

        if show_table_h:
            st.subheader("Raw hourly (из БД)")
            st.dataframe(dfh, width="stretch")
