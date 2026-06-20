# Protected Areas ETL

An end-to-end geospatial pipeline that monitors deforestation risk in Brazilian indigenous territories using Sentinel-1 SAR data from Microsoft Planetary Computer.

For each territory, the pipeline:
1. Fetches the boundary from a PostGIS database
2. Queries Sentinel-1 SAR backscatter items via the STAC API
3. Loads and processes the datacube (dB conversion, vegetation indices)
4. Resamples to weekly or monthly periods and computes spatial means
5. Ingests the timeseries and acquisition metadata back into the database

---

## Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 3.x (CeleryExecutor) |
| Task queue | Redis |
| Data warehouse | PostgreSQL 16 + PostGIS 3.4 |
| Airflow metadata DB | PostgreSQL 16 |
| SAR data source | Microsoft Planetary Computer (Sentinel-1 RTC) |
| STAC client | pystac-client + planetary-computer |
| Datacube processing | odc-stac, xarray, rioxarray, Dask |
| Geospatial | GDAL, rasterio, shapely, geopandas |
| Package management | uv |
| Containers | Docker + Docker Compose |

---

## Database Architecture

```
geopipeline (PostGIS)
│
├── land
│   ├── terrai_id       INTEGER  PK
│   ├── terrai_name     VARCHAR        -- e.g. "Rio Pindaré"
│   ├── tribe_name      VARCHAR
│   ├── state_uf        VARCHAR        -- Brazilian state abbreviation
│   ├── modality        VARCHAR
│   ├── on_border       VARCHAR
│   └── geometry        MULTIPOLYGON (EPSG:4326)
│
├── satellite_acquisitions
│   ├── id              SERIAL   PK
│   ├── item_id         VARCHAR        -- STAC item ID
│   ├── collection      VARCHAR        -- e.g. "sentinel-1-rtc"
│   ├── acquired_at     TIMESTAMPTZ
│   ├── platform        VARCHAR        -- sentinel-1a / sentinel-1b
│   ├── orbit_direction VARCHAR        -- ascending / descending
│   ├── relative_orbit  INTEGER
│   ├── look_angle      REAL
│   ├── scene_bbox      POLYGON (EPSG:4326)
│   ├── terrai_id       INTEGER  FK → land
│   └── ingested_at     TIMESTAMPTZ
│
└── satellite_timeseries
    ├── id              SERIAL   PK
    ├── date            DATE
    ├── terrai_id       INTEGER  FK → land
    ├── vv              REAL     -- VV backscatter (dB)
    ├── vh              REAL     -- VH backscatter (dB)
    ├── vv_vh_ratio     REAL     -- VV/VH ratio
    └── rvi             REAL     -- Radar Vegetation Index
```

`land` is populated once by the ingestor container on first startup. `satellite_acquisitions` and `satellite_timeseries` are populated by the Airflow DAG on each run. Both tables use `ON CONFLICT DO NOTHING` so re-runs are safe.

---

## Running

### Prerequisites

- Docker and Docker Compose installed
- A `.env` file in the repo root (see `.env` for the required variables)

### Start

```bash
# First time or after changing Dockerfile / pyproject.toml
make up-build

# Subsequent starts
make up
```

`airflow-init` runs automatically before any other Airflow service. It creates the required directories, sets file ownership, and migrates the Airflow metadata database. The ingestor runs once and loads the GeoJSON data into the `land` table.

### Stop

```bash
make down          # stops containers, keeps database volumes
make down-volumes  # stops containers and deletes all data
```

---

## Accessing Airflow

Open **http://localhost:8081** in your browser.

On first startup, `SimpleAuthManager` generates a random password for the `admin` user. Retrieve it with:

```bash
docker compose exec airflow-apiserver cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

Log in with username `admin` and the password from that file.

---

## Running a DAG

The pipeline is defined in `dags/dag.py` as the `protected_areas_etl` DAG.

1. In the Airflow UI, go to **DAGs** and find `protected_areas_etl`
2. Toggle it **on** if it is paused
3. Click **Trigger DAG w/ config** and set the parameters:

| Parameter | Default | Description |
|---|---|---|
| `protected_area` | `Rio Pindaré` | Territory name (must match `terrai_name` in the `land` table) |
| `collection` | `sentinel-1-rtc` | STAC collection |
| `initial_date` | `2025-01-01` | Start date |
| `final_date` | `2025-06-30` | End date |
| `bands` | `["vv", "vh"]` | SAR polarization bands |
| `resolution` | `30` | Spatial resolution in metres |
| `resampling_time` | `W` | Resampling period (`W` = weekly, `M` = monthly) |

4. Click **Trigger**

Monitor progress in the **Grid** or **Graph** view. Task logs are available by clicking any task box.

---

## Database Access

### DBeaver (or any SQL client)

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `5434` |
| Database | `geopipeline` |
| Username | `geouser` |
| Password | `geoadmin` |

### Terminal

```bash
make psql
```

### Useful queries

Areas in Amazonas state smaller than 5 km²:

```sql
SELECT *
FROM (
    SELECT
        *,
        ST_Area(geometry::geography) / 1e6 AS area_km2
    FROM land
    WHERE state_uf = 'AM'
) sub
WHERE area_km2 < 5
LIMIT 10;
```

Timeseries for a specific territory:

```sql
SELECT date, vv, vh, rvi
FROM satellite_timeseries
WHERE terrai_id = (SELECT terrai_id FROM land WHERE terrai_name = 'Rio Pindaré')
ORDER BY date;
```

---

## Makefile reference

| Command | Description |
|---|---|
| `make build` | Build images without starting |
| `make up` | Start all services |
| `make up-build` | Rebuild images and start |
| `make down` | Stop, keep volumes |
| `make down-volumes` | Stop and delete all data |
| `make kill` | Full teardown including images (asks for confirmation) |
| `make logs` | Tail all logs |
| `make logs-worker` | Tail worker logs |
| `make logs-scheduler` | Tail scheduler logs |
| `make restart-worker` | Restart the Celery worker |
| `make psql` | Open a psql shell in the data warehouse |
