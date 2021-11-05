#FOLLOWING ookla tutorial: https://github.com/teamookla/ookla-open-data/blob/master/tutorials/aggregate_by_county_py.ipynb

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
# import contextily as ctx cannot install
import json
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from shapely.geometry import Point
from datetime import datetime

def quarter_start(year: int, q: int) -> datetime:
    if not 1 <= q <= 4:
        raise ValueError("Quarter must be within [1, 2, 3, 4]")

    month = [1, 4, 7, 10]
    return datetime(year, month[q - 1], 1)


def get_tile_url(service_type: str, year: int, q: int) -> str:
    dt = quarter_start(year, q)

    base_url = "https://ookla-open-data.s3-us-west-2.amazonaws.com/shapefiles/performance"
    url = f"{base_url}/type%3D{service_type}/year%3D{dt:%Y}/quarter%3D{q}/{dt:%Y-%m-%d}_performance_fixed_tiles.zip"
    return url

def get_tile(file=None):
    if file is None:
        tile_url = get_tile_url("fixed", 2021, 1)
        print(tile_url)
        tiles = gpd.read_file(tile_url)
    else:
        tiles = gpd.read_file(file)
    print("ookla data: ", tiles.head(), tiles.columns)
    print(type(tiles))
    return tiles 

def pickle_ookla_tiles(geo_df):
    geo_df.to_pickle("ookla.pkl")

def get_kentucky_boundary(file=None):
    if file is None: 
        # zipfile of U.S. county boundaries
        county_url = "https://www2.census.gov/geo/tiger/TIGER2019/COUNTY/tl_2019_us_county.zip" 
        counties = gpd.read_file(county_url)
    else:
        counties = gpd.read_file(file)
    # filter out the Kentucky fips code and reproject to match the tiles
    ky_counties = counties.loc[counties['STATEFP'] == '21'].to_crs(4326)
    print("county bd: ", ky_counties.head(), ky_counties.columns)
    return ky_counties

def join_ookla_tile_with_kentucky_bd(tiles,ky_counties, how="inner", spatial_join = "intersects" ):
    #tutorial:  how = "inner" -> only want to include counties that have at least 1 tile.
    #‘inner’: use intersection of keys from both dfs; retain only left_df geometry column
    # so the geom only holds the tiles geom 
    tiles_in_ky_counties = gpd.sjoin(tiles, ky_counties, how=how, op=spatial_join)
    # convert to Mbps for easier reading
    tiles_in_ky_counties['avg_d_mbps'] = tiles_in_ky_counties['avg_d_kbps'] / 1000
    tiles_in_ky_counties['avg_u_mbps'] = tiles_in_ky_counties['avg_u_kbps'] / 1000
    tiles_in_ky_counties.head()
    return tiles_in_ky_counties

def pickle_joined_tile():
    ookla_file = r"D:\Docs\compete\datakind\bband\jlRepo\2021-01-01_performance_fixed_tiles.zip"
    county_file = r"D:\Docs\compete\datakind\bband\jlRepo\tl_2019_us_county.zip"
    tiles = get_tile(ookla_file)
    ky_bd=get_kentucky_boundary(county_file)
    res = join_ookla_tile_with_kentucky_bd(tiles, ky_bd)
    res.to_pickle("res.pkl")
    print("joined_tile: ", res.head())

def pickle_county_file(file):
    ky_places = gpd.read_file(file)
    ky_places = ky_places.sample(15, random_state=1).to_crs(26916)
    ky_places["centroid"] = ky_places["geometry"].centroid
    ky_places.set_geometry("centroid", inplace = True)
    #save
    ky_places.to_pickle("ky_county_lines.pkl")

def plot_ky(pickle_file):
    fig, ax = plt.subplots(1, figsize=(16, 6))
    ky_places = pd.read_pickle(pickle_file)
    ky_places.plot(linewidth=0.4, ax=ax, edgecolor="0.1")
    fig.savefig("ky_county_lines.png")

def plot_ookla_geom_ints_ky(pickle_file):
    ookla_geom_df = pd.read_pickle(pickle_file)
    fig, ax = plt.subplots(1, figsize=(16, 6))

    ookla_geom_df.plot(
        column="avg_d_mbps", cmap="BuPu", linewidth=0.4, ax=ax, edgecolor="0.1", legend=True
    )
    fig.savefig("ookla_geom_ints_ky.png")

if __name__ == "__main__":
    gdf = pd.read_pickle("ookla.pkl")
    print(type(gdf))
    print(gdf.head(5))
    print(type(gdf.geometry[0]))
    try:
        print(gdf.crs)
    except Exception:
        print("no crs")   
    fig, ax = plt.subplots(1)
    gdf.set_geometry('geometry')
    gdf[:150].plot(linewidth=0.4, ax=ax, edgecolor="0.1") #draws ookla_tiles.png



