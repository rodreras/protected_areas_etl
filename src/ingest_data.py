import logging 
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from typing import List, Any

logger = logging.getLogger()
logging.basicConfig(
        level = logging.INFO, 
        format = "%(asctime)s [%(levelname)s] %(message)s")


def concat_timeseries(dfs: List) -> pd.DataFrame:
    for df in dfs:
        df['date'] = pd.to_datetime(df['time']).dt.strftime('%Y-%m-%d')
        df.drop(columns='time', inplace = True)
        date_col = df.pop('date')
        df.insert(0, 'date', date_col)

    return pd.concat(dfs, axis = 1, join = 'outer').T.drop_duplicates().T

def get_connection(user:str, password:str, host:str, port:int, database:str):
    logging.info('Created DB Engine.')
    return create_engine(
        url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    )

def to_db(
        df: pd.DataFrame| gpd.GeoDataFrame,
        name: str, 
        con: Any, 
        if_exists: str = 'append', 
        index: bool = False
        ):
    if isinstance(df, gpd.GeoDataFrame):
        df.to_postgis(name=name, con=con, if_exists=if_exists, index=index)
    elif isinstance(df, pd.DataFrame):
        df.to_sql(name=name, con=con, if_exists=if_exists, index=index)
    return logger.info('Data loaded to database successfully!')