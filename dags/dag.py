from __future__ import annotations

import logging
import os
import sys
from typing import Any
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context


logger = logging.getLogger(__name__)

load_dotenv()

def _db_engine():
    """Build a SQLAlchemy engine from the env vars injected by docker-compose."""
    from sqlalchemy import create_engine

    return create_engine(
        "postgresql://{}:{}@{}:{}/{}".format(
            os.environ.get("POSTGRES_USER"),
            os.environ.get("POSTGRES_PASSWORD"),
            os.environ.get("POSTGRES_HOST"),
            os.environ.get("POSTGRES_PORT"),
            os.environ.get("POSTGRES_DB"),
        )
    )


@dag(
    dag_id="protected_areas_etl",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["STAC", "Sentinel-1", "Catalog"],
    params={
        "protected_area": Param(
            "Rio Pindaré",
            type="string",
            description="Indigenous land name (terrai_name column in the land table).",
        ),
        "collection": Param(
            "sentinel-1-rtc",
            type="string",
            description="STAC collection ID to query.",
        ),
        "initial_date": Param(
            "2025-01-01",
            type="string",
            description="Start date (YYYY-MM-DD).",
        ),
        "final_date": Param(
            "2025-06-30",
            type="string",
            description="End date (YYYY-MM-DD).",
        ),
        "bands": Param(
            ["vv", "vh"],
            type="array",
            description="SAR polarization bands to load.",
        ),
        "resolution": Param(
            30,
            type="integer",
            description="Spatial resolution in metres.",
        ),
        "resampling_time": Param(
            "W",
            type="string",
            description="Temporal resampling period (W = weekly, M = monthly).",
        ),
    },
)
def protected_areas_etl() -> None:

    @task()
    def get_land_task() -> dict[str, Any]:
        """Fetch the boundary and metadata for the target land from PostGIS."""
        from sqlalchemy import text

        ctx = get_current_context()
        protected_area: str = ctx["params"]["protected_area"]

        query = text("""
            SELECT
                terrai_id,
                terrai_name,
                tribe_name,
                state_uf,
                ST_AsGeoJSON(geometry) AS geometry_geojson
            FROM land
            WHERE terrai_name = :name
            LIMIT 1
        """)
        with _db_engine().connect() as conn:
            row = conn.execute(query, {"name": protected_area}).mappings().fetchone()

        if row is None:
            raise ValueError(
                f"Protected area '{protected_area}' not found in the land table. "
                "Check the terrai_name column for the exact spelling."
            )

        logger.info(
            "Loaded land record — terrai_id=%d  name='%s'  state=%s",
            row["terrai_id"], row["terrai_name"], row["state_uf"],
        )
        return dict(row)

    @task()
    def get_stac_task(land: dict[str, Any]) -> list[dict[str, Any]]:
        """Query Planetary Computer STAC and return items as JSON-serialisable dicts."""
        import json

        import planetary_computer
        from pystac_client import Client

        ctx = get_current_context()
        collection: str = ctx["params"]["collection"]
        initial_date: str = ctx["params"]["initial_date"]
        final_date: str = ctx["params"]["final_date"]

        catalog = Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1",
            modifier=planetary_computer.sign_inplace,
        )
        geometry = json.loads(land["geometry_geojson"])

        item_collection = catalog.search(
            collections=[collection],
            datetime=f"{initial_date}/{final_date}",
            intersects=geometry,
        ).item_collection()

        logger.info(
            "Found %d STAC items for '%s' (%s → %s).",
            len(item_collection), land["terrai_name"], initial_date, final_date,
        )
        return [item.to_dict() for item in item_collection]

    @task()
    def process_stac_task(
        items_dicts: list[dict[str, Any]],
        land: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Load the SAR cube, compute backscatter indexes, resample, return spatial means.

        configure_rio(cloud_defaults=True) is required before stac_load so that
        GDAL/rasterio uses cloud-optimized HTTP range-request settings when reading
        from Planetary Computer (Azure Blob). Without it, .compute() hangs on the
        first network read because GDAL falls back to non-streaming behaviour.
        """
        import json

        import numpy as np
        import pystac
        import rioxarray
        from odc.stac import configure_rio, stac_load
        from shapely.geometry import shape

        from src import process_stac

        ctx = get_current_context()
        bands: list[str] = ctx["params"]["bands"]
        resolution: int = ctx["params"]["resolution"]
        resampling_time: str = ctx["params"]["resampling_time"]

        if not items_dicts:
            logger.warning("No STAC items to process for terrai_id=%d.", land["terrai_id"])
            return []

        # Must be called before stac_load so GDAL uses cloud-optimized defaults
        configure_rio(cloud_defaults=True)

        geometry_dict = json.loads(land["geometry_geojson"])
        shapely_geom = shape(geometry_dict)
        items = [pystac.Item.from_dict(d) for d in items_dicts]

        logger.info(
            "Loading %d items (bands=%s, resolution=%dm)...", len(items), bands, resolution,
        )
        s1_cube = stac_load(
            items=items,
            bands=bands,
            intersects=geometry_dict,
            resolution=resolution,
            chunks={},
            preserve_original_order=True,
            groupby="solar_day",
        )

    
        s1_cube = s1_cube.rio.clip([shapely_geom], crs=4326)

        s1_cube = s1_cube.where(s1_cube > 0)           # mask no-data before log-transform
        s1_cube = process_stac.apply_db_scale(s1_cube)  # vv, vh → dB
        s1_cube = process_stac.vv_vh_ratio(s1_cube)     # vv_vh_ratio added
        s1_cube = process_stac.rvi(s1_cube)              # rvi added

        resampled = s1_cube.resample(time=resampling_time).mean()
        spatial_mean = resampled.mean(dim=["x", "y"]).compute()

        def _safe(val: Any) -> float | None:
            f = float(val)
            return None if np.isnan(f) else f

        records: list[dict[str, Any]] = []
        for t in spatial_mean.time.values:
            ts = spatial_mean.sel(time=t)
            records.append({
                "date": str(t)[:10],
                "terrai_id": land["terrai_id"],
                "vv": _safe(ts["vv"].values),
                "vh": _safe(ts["vh"].values),
                "vv_vh_ratio": _safe(ts["vv_vh_ratio"].values),
                "rvi": _safe(ts["rvi"].values),
            })

        logger.info(
            "Produced %d timeseries records for terrai_id=%d.", len(records), land["terrai_id"],
        )
        return records

    @task()
    def extract_metadata_task(
        items_dicts: list[dict[str, Any]],
        land: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Extract per-acquisition metadata from STAC item properties."""
        records: list[dict[str, Any]] = []
        for item in items_dicts:
            props = item.get("properties", {})
            bbox = item.get("bbox")
            records.append({
                "item_id": item["id"],
                "collection": item.get("collection", ""),
                "acquired_at": props.get("datetime"),
                "platform": props.get("platform"),
                "orbit_direction": props.get("sat:orbit_state"),
                "relative_orbit": props.get("sat:relative_orbit"),
                "look_angle": props.get("view:off_nadir"),
                "bbox_west": float(bbox[0]) if bbox else None,
                "bbox_south": float(bbox[1]) if bbox else None,
                "bbox_east": float(bbox[2]) if bbox else None,
                "bbox_north": float(bbox[3]) if bbox else None,
                "terrai_id": land["terrai_id"],
            })

        logger.info("Extracted metadata for %d acquisitions.", len(records))
        return records

    @task()
    def ingest_timeseries_task(records: list[dict[str, Any]]) -> int:
        """Upsert timeseries rows — skips duplicates via the (date, terrai_id) constraint."""
        from sqlalchemy import text

        if not records:
            logger.warning("No timeseries records to ingest.")
            return 0

        sql = text("""
            INSERT INTO satellite_timeseries (date, terrai_id, vv, vh, vv_vh_ratio, rvi)
            VALUES (:date, :terrai_id, :vv, :vh, :vv_vh_ratio, :rvi)
            ON CONFLICT (date, terrai_id) DO NOTHING
        """)
        with _db_engine().begin() as conn:
            result = conn.execute(sql, records)

        logger.info("Inserted %d/%d timeseries rows.", result.rowcount, len(records))
        return result.rowcount

    @task()
    def ingest_metadata_task(records: list[dict[str, Any]]) -> int:
        """Upsert acquisition metadata — skips duplicates via the (item_id, terrai_id) constraint."""
        from sqlalchemy import text

        if not records:
            logger.warning("No metadata records to ingest.")
            return 0

        sql = text("""
            INSERT INTO satellite_acquisitions (
                item_id, collection, acquired_at, platform,
                orbit_direction, relative_orbit, look_angle,
                scene_bbox, terrai_id
            ) VALUES (
                :item_id, :collection, :acquired_at, :platform,
                :orbit_direction, :relative_orbit, :look_angle,
                ST_MakeEnvelope(:bbox_west, :bbox_south, :bbox_east, :bbox_north, 4326),
                :terrai_id
            )
            ON CONFLICT (item_id, terrai_id) DO NOTHING
        """)
        with _db_engine().begin() as conn:
            result = conn.execute(sql, records)

        logger.info("Inserted %d/%d acquisition metadata rows.", result.rowcount, len(records))
        return result.rowcount

    # Task wiring 
    land = get_land_task()
    items = get_stac_task(land)

    timeseries = process_stac_task(items, land)
    metadata = extract_metadata_task(items, land)

    ingest_timeseries_task(timeseries)
    ingest_metadata_task(metadata)


dag_instance = protected_areas_etl()
