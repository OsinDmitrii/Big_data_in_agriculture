# Big_data_in_agriculture

- **t2m** — температура воздуха на высоте 2 м *(в Кельвинах; ты перевёл в °C)*.
- **d2m** — точка росы на 2 м *(в К)*. По ней видно “влажность” воздуха: чем ближе к **t2m**, тем влажнее.
- **tp** — суммарные осадки за шаг времени (обычно за час) *(в метрах воды; ты перевёл в мм ×1000)*.
- **u10, v10** — компоненты ветра на 10 м *(м/с)* по осям **запад–восток** и **юг–север**.
- **wind_speed_10m** — скорость ветра *(м/с)*, рассчитана как: `sqrt(u10^2 + v10^2)`.
- **swvl1** — объёмная влажность почвы, слой 1 (верхний слой) *(м³/м³; доля воды в почве)*.
- **swvl2** — объёмная влажность почвы, слой 2 (глубже) *(м³/м³)*.
- **stl2** — температура почвы, уровень 2 *(в К; отражает прогрев/промерзание почвы)*.

# Big_data_in_agriculture — ERA5-Land ETL + Dashboard

## 1) Что делает проект

Проект строит витрины данных (marts) по **ERA5-Land** для набора регионов: скачиваем «сырьё» (NetCDF/ZIP), агрегируем до **hourly/daily** по региону, загружаем в **PostgreSQL**, показываем в **Streamlit**.

---

## 2) Компоненты системы

- **PostgreSQL (`agri-postgres`)** — хранение витрин в схеме `marts` (`era5_hourly`, `era5_daily`, …).  
  Порт наружу: `5433 -> 5432`.

- **Dask (`agri-dask-scheduler`, `agri-dask-worker`)** — выполнение тяжёлого transform (агрегация NetCDF → Parquet).  
  Dashboard: `http://localhost:8787`, scheduler: `8786`.

- **Prefect (`agri-prefect-server`, `agri-prefect-worker`)** — оркестрация/наблюдение (UI), в будущем можно собрать единый flow “extract → transform → load”.  
  UI/API: `http://localhost:4200`.

- **Python скрипты (`flows/` и `dask_jobs/`)** — собственно ETL-логика.

- **Streamlit (`dashboards/app.py`)** — визуализация данных из Postgres.

---

## 3) Структура проекта (важное)

- `config/regions.yaml` — регионы и bounding box  
  `area: [north, west, south, east]`

- `flows/`
  - `download_era5_land.py` — **EXTRACT** (скачивание raw)
  - `load_hourly_parquet_to_postgres.py` — **LOAD hourly**
  - `load_daily_parquet_to_postgres.py` — **LOAD daily**

- `dask_jobs/`
  - `aggregate_hourly.py` — **TRANSFORM hourly** (NetCDF/ZIP → Parquet)
  - `aggregate_daily.py` — **TRANSFORM daily** (дневные агрегаты; если не запускали — daily в БД пустой)

- `data/`
  - `raw/era5-land/region=…/year=…/month=..(nc|zip)` — сырьё
  - `marts/hourly/region=…/year=…/month=..parquet` — витрина hourly в файлах
  - `marts/daily/...` — витрина daily в файлах (после `aggregate_daily`)

- `docker/init/*.sql` — создание схем/таблиц `marts.*` при старте Postgres
- `docker-compose.yml` — все сервисы

---

## 4) Модель данных и преобразования

**RAW ERA5-Land**: измерения на сетке `latitude × longitude` во времени `valid_time/time`.

В transform делаем:
- **Агрегация по региону**: `mean` по `lat/lon` для каждого timestamp.
- **Конверсия единиц** (типовая логика):
  - `t2m`, `d2m`: K → °C (`-273.15`)
  - `tp`: м воды → мм (`*1000`)
  - `wind_speed_10m`: `sqrt(u10² + v10²)`

**Hourly витрина**: регион × час (`region`, `ts`, `t2m`, `tp`, `swvl1`, …)  
**Daily витрина**: регион × день (`region`, `day`, `t2m_mean/min/max`, `tp_sum`, …)

# Инструкция по запуску
---
### 0) Поднять весь проект

```bash
docker compose up -d
docker compose ps
```
---

### A) EXTRACT (скачать сырьё в data/raw/era5-land)

**--limit-days 0 = весь месяц**

```bash
python flows/download_era5_land.py \
  --year 2022 \
  --months 1,2,3,4,5,6,7,8,9,10,11,12 \
  --limit-days 0 \
  --vars t2m,d2m,tp,u10,v10,swvl1,swvl2
```

---

### Проверка

```bash
ls -la data/raw/era5-land/region=bashkortostan/year=2022 | head
```

---

### B) TRANSFORM (агрегация → parquet в data/marts/hourly)

```bash
docker exec -it -w /opt/app agri-dask-worker \
  python dask_jobs/aggregate_hourly.py \
  --year 2022 \
  --months 1,2,3,4,5,6,7,8,9,10,11,12 \
  --dask tcp://dask-scheduler:8786
```

---

### Проверка

```bash
docker exec -it -w /opt/app agri-dask-worker \
  ls -la data/marts/hourly/region=bashkortostan/year=2022 | head
```

---

### C) LOAD (залить parquet в Postgres)

```bash
PGHOST=127.0.0.1 PGPORT=5433 PGDATABASE=agri PGUSER=agri PGPASSWORD=agri \
python flows/load_hourly_parquet_to_postgres.py \
  --year 2022 \
  --months 1,2,3,4,5,6,7,8,9,10,11,12
```

---

### Проверка

```bash
PGPASSWORD=agri psql -h 127.0.0.1 -p 5433 -U agri -d agri -c \
"select region, count(*) as rows, min(ts) as min_ts, max(ts) as max_ts
 from marts.era5_hourly
 group by region
 order by region;"
```

```bash
PGPASSWORD=agri psql -h 127.0.0.1 -p 5433 -U agri -d agri -c \
"select * from marts.era5_hourly limit 5;"
```

```bash
PGPASSWORD=agri psql -h 127.0.0.1 -p 5433 -U agri -d agri -c \
"select column_name, data_type
 from information_schema.columns
 where table_schema='marts' and table_name='era5_hourly'
 order by ordinal_position;"
```

---

### D) Визуализация

```bash
mkdir -p ~/.streamlit
cat > ~/.streamlit/config.toml <<'EOF'
[browser]
gatherUsageStats = false
EOF
```

```bash
pip install -U streamlit sqlalchemy psycopg2-binar # возможно нужно
```

### Запуск

```bash
PGHOST=127.0.0.1 PGPORT=5433 PGDATABASE=agri PGUSER=agri PGPASSWORD=agri \
python -m streamlit run dashboards/app.py
```

---

### Наблюдение и UI

Prefect UI: http://localhost:4200

Dask UI: http://localhost:8787

Streamlit UI: http://localhost:8501 






