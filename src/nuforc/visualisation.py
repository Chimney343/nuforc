import pickle

import geopandas as gpd
import pandas as pd


def make_playset(events_path, geocoded_path):
    with open(events_path, "rb") as pickle_file:
        data = pickle.load(pickle_file)
    events = pd.DataFrame(data)

    with open(geocoded_path, "rb") as pickle_file:
        data = pickle.load(pickle_file)
    geocoded = pd.DataFrame(data)

    df = events.merge(geocoded, on=["city", "state", "state_abbreviation", "country"])
    return df


def gdf_to_geo_file(gdf, output_path, filetype=None):
    if not filetype:
        gdf.to_file(output_path)
