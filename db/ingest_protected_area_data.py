import geopandas as gpd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

database = os.environ.get("POSTGRES_DB")
user     = os.environ.get("POSTGRES_USER")
password = os.environ.get("POSTGRES_PASSWORD")
port     = os.environ.get("POSTGRES_PORT", "5432")
host     = os.environ.get("POSTGRES_HOST", "data-warehouse")  # Docker service name

# Build explicit engine pointing at data-warehouse
engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
)

data_path = "./data/indigenous_protected_area.geojson"
gdf = gpd.read_file(data_path)

gdf = gdf[[
    "terrai_codigo", "terrai_nome",
    "etnia_nome", "uf_sigla",
    "modalidade_ti", "faixa_fronteira",
    "geometry"
]]

rename_dict = {
    "terrai_codigo": "terrai_id",
    "terrai_nome":   "terrai_name",
    "etnia_nome":    "tribe_name",
    "uf_sigla":      "state_uf",
    "modalidade_ti": "modality",
    "faixa_fronteira": "on_border"
}
gdf.rename(columns=rename_dict, inplace=True)
gdf["terrai_id"] = gdf["terrai_id"].astype(int)

print(f"Connecting to: {host}:{port}/{database} as {user}")
print(f"Rows to ingest: {len(gdf)}")

gdf.to_postgis(
    name="land",
    con=engine,
    if_exists="append",
    index=False
)

print("Done — land table populated.")