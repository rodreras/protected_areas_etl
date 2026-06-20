import logging
import xarray as xr
import pandas as pd
import numpy as np

logger = logging.getLogger()
logging.basicConfig(
        level = logging.INFO, 
        format = "%(asctime)s [%(levelname)s] %(message)s")

def _db_scale(x: float) -> float:
    return 10 * np.log10(x)

def vv_vh_ratio(cube: xr.Dataset) -> xr.Dataset:
    cube['vv_vh_ratio'] = cube['vv'] / cube['vh']
    return cube

def rvi(cube: xr.Dataset) -> xr.Dataset: 
    cube['rvi'] = (4 * cube['vh']) / (cube['vv'] + cube['vh'])
    return cube

def apply_db_scale(cube: xr.Dataset) -> xr.Dataset:
    for band in cube.data_vars:
        cube[band] = _db_scale(cube[band])
    return cube

def resample_dataset(cube, time = 'W'):
    ''' Resamples data in specific time period. Default: W (week)'''
    return cube.resample(time = time).mean()

def timeseries_difference(cube: xr.Dataset, band: str):
    '''Computs the difference of the 1st and last image in the time series.'''
    return cube[band].isel(time = -1) - cube[band].isel(time = 0)

def cube_to_dataframe(cube: xr.Dataset, band: str) -> pd.DataFrame:
    '''Transform band information to a dataframe.'''
    return cube[band].to_dataframe().dropna()

def mean_of_period(table: pd.DataFrame, band: str) -> pd.DataFrame:
    '''Group by the time by a specific band and performs the mean. Output is the mean for the whole image.'''
    return table.groupby('time')[band].mean()
