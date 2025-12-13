# Big_data_in_agriculture
0) Поднять весь проект
docker compose up -d
docker compose ps

A) EXTRACT (скачать сырьё в data/raw/era5-land)
--limit-days 0 = весь месяц

python flows/download_era5_land.py \
  --year 2022 \
  --months 1,2,3,4,5,6,7,8,9,10,11,12 \
  --limit-days 0 \
  --vars t2m,d2m,tp,u10,v10,swvl1,swvl2

Проверка
ls -la data/raw/era5-land/region=bashkortostan/year=2022 | head

B) TRANSFORM (агрегация → parquet в data/marts/hourly)
docker exec -it -w /opt/app agri-dask-worker \
  python dask_jobs/aggregate_hourly.py \
  --year 2022 \
  --months 1,2,3,4,5,6,7,8,9,10,11,12 \
  --dask tcp://dask-scheduler:8786

Проверка
docker exec -it -w /opt/app agri-dask-worker \
  ls -la data/marts/hourly/region=bashkortostan/year=2022 | head

C) LOAD (залить parquet в Postgres)
PGHOST=127.0.0.1 PGPORT=5433 PGDATABASE=agri PGUSER=agri PGPASSWORD=agri \
python flows/load_hourly_parquet_to_postgres.py \
  --year 2022 \
  --months 1,2,3,4,5,6,7,8,9,10,11,12


Проверка
PGPASSWORD=agri psql -h 127.0.0.1 -p 5433 -U agri -d agri -c \
"select region, count(*) as rows, min(ts) as min_ts, max(ts) as max_ts
 from marts.era5_hourly
 group by region
 order by region;"


PGPASSWORD=agri psql -h 127.0.0.1 -p 5433 -U agri -d agri -c \
"select * from marts.era5_hourly limit 5;"


PGPASSWORD=agri psql -h 127.0.0.1 -p 5433 -U agri -d agri -c \
"select column_name, data_type
 from information_schema.columns
 where table_schema='marts' and table_name='era5_hourly'
 order by ordinal_position;"


D) Визуализация

mkdir -p ~/.streamlit
cat > ~/.streamlit/config.toml <<'EOF'
[browser]
gatherUsageStats = false
EOF

pip install -U streamlit sqlalchemy psycopg2-binar # возможно нужно


Запуск
PGHOST=127.0.0.1 PGPORT=5433 PGDATABASE=agri PGUSER=agri PGPASSWORD=agri \
python -m streamlit run dashboards/app.py


