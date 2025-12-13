CREATE SCHEMA IF NOT EXISTS marts;

-- (опционально) таблица под почасовую витрину, если решишь грузить parquet в БД
CREATE TABLE IF NOT EXISTS marts.fact_hourly (
  region TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  t2m DOUBLE PRECISION,
  d2m DOUBLE PRECISION,
  tp DOUBLE PRECISION,
  u10 DOUBLE PRECISION,
  v10 DOUBLE PRECISION,
  swvl1 DOUBLE PRECISION,
  swvl2 DOUBLE PRECISION,
  wind_speed_10m DOUBLE PRECISION,
  PRIMARY KEY (region, ts)
);
