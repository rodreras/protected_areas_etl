CREATE EXTENSION IF NOT EXISTS postgis;

-- Indigenous land boundaries and metadata (populated by db/ingest_protected_area_data.py)
CREATE TABLE IF NOT EXISTS land (
    terrai_id   INTEGER      PRIMARY KEY,
    terrai_name VARCHAR(255) NOT NULL,
    tribe_name  VARCHAR(255) NOT NULL,
    state_uf    VARCHAR(255) NOT NULL,
    modality    VARCHAR(255) NOT NULL,
    on_border   VARCHAR(255) NOT NULL,
    geometry    GEOMETRY(MULTIPOLYGON, 4326)
);

CREATE INDEX IF NOT EXISTS land_geometry_idx ON land USING GIST (geometry);

-- One row per satellite acquisition that intersects a land boundary.
-- Lets us track which passes were processed and diagnose backscatter anomalies
-- (e.g. orbit direction changes, look-angle variation across dates).
CREATE TABLE IF NOT EXISTS satellite_acquisitions (
    id              SERIAL       PRIMARY KEY,
    item_id         VARCHAR(255) NOT NULL,
    collection      VARCHAR(255) NOT NULL,
    acquired_at     TIMESTAMPTZ  NOT NULL,
    platform        VARCHAR(100),                  -- e.g. sentinel-1a / sentinel-1b
    orbit_direction VARCHAR(20),                   -- ascending | descending
    relative_orbit  INTEGER,
    look_angle      REAL,                          -- off-nadir angle in degrees
    scene_bbox      GEOMETRY(POLYGON, 4326),       -- footprint of the acquisition
    terrai_id       INTEGER      NOT NULL REFERENCES land(terrai_id),
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (item_id, terrai_id)
);

CREATE INDEX IF NOT EXISTS acq_terrai_time_idx ON satellite_acquisitions (terrai_id, acquired_at);
CREATE INDEX IF NOT EXISTS acq_bbox_idx        ON satellite_acquisitions USING GIST (scene_bbox);

-- Spatially averaged SAR backscatter timeseries per land boundary.
-- Each row is the mean value of all valid pixels inside the territory for one resampling period.
CREATE TABLE IF NOT EXISTS satellite_timeseries (
    id          SERIAL  PRIMARY KEY,
    date        DATE    NOT NULL,
    terrai_id   INTEGER NOT NULL REFERENCES land(terrai_id),
    vv          REAL,          -- VV backscatter (dB)
    vh          REAL,          -- VH backscatter (dB)
    vv_vh_ratio REAL,          -- VV/VH ratio (dB space)
    rvi         REAL,          -- Radar Vegetation Index
    UNIQUE (date, terrai_id)
);

CREATE INDEX IF NOT EXISTS ts_terrai_date_idx ON satellite_timeseries (terrai_id, date);
