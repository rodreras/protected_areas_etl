import logging 
from typing import List, Dict, Any
import geopandas as gpd
from shapely import Polygon
from pystac_client import Client


logger = logging.getLogger()
logging.basicConfig(
        level = logging.INFO, 
        format = "%(asctime)s [%(levelname)s] %(message)s")

def query_stac_catalog(
        catalog: Client,
        collection: List[str],
        datetime_range: str,
        filter: Dict[Any, Any],
        geometry: Polygon
    ) -> Dict:
    return catalog.search(
        collections = [collection],
        datetime = datetime_range,
        query = filter,
        intersects = geometry
        ).items()

def catalog_items_as_gdf(query: Any, epsg: str = 'epsg:4326') -> gpd.GeoDataFrame:
    json = query.item_collection_as_dict()
    return gpd.GeoDataFrame.from_features(json, epsg)
