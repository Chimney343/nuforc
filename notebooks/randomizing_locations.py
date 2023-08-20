#!/usr/bin/env python
# coding: utf-8

# In[7]:


import logging.config
import pickle
from pathlib import Path
from pprint import pprint
import sys
import numpy as np
import pandas as pd
from dateutil.parser import parse
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from tqdm.autonotebook import tqdm
sys.path.append('..')
from IPython.display import display
import random
import math
from tqdm.autonotebook import tqdm
from dotenv import load_dotenv
from src.nuforc import SETTINGS
from src.nuforc.utility import *
from src.nuforc.geocoding.geocoder import Geolocator
from src.nuforc.geocoding.wrangling import replace_unparsed_with_none, join_columns
from datetime import date
import geopandas as gpd
from shapely.geometry import Point
from shapely.geometry import Polygon
from geopandas import GeoSeries
from scipy.stats import mode
load_dotenv()
import os


# #### Data loading

# In[8]:


# Events
raw = pd.read_csv(Path(os.getenv('DATA_DIR')) / 'raw_events' / 'events_2023_07_21.csv')
raw = replace_unparsed_with_none(raw)
raw['address'] = raw.apply(lambda row: join_columns(row['city'], row['state'], row['country']), axis=1)

# Coords
coords = pd.read_csv(Path(os.getenv('DATA_DIR')) / 'gis' / 'csv' / 'geolocated_addresses.csv', names=['address', 'latitude', 'longitude'])


# In[10]:


raw.to_csv(Path(os.getenv('DATA_DIR')) / 'raw_events' / 'events_2023_07_21.csv', index=False)


# In[4]:


# Zipcode polygons
polygons = gpd.read_file(Path(os.getenv('DATA_DIR')) / 'gis' / 'shp' / 'cb_2018_us_zcta510_500k.shp')


# In[188]:


# Define a function to lowercase strings while handling None values
def lowercase_string(s):
    if isinstance(s, str):
        return s.lower()
    else:
        return s

df = raw.copy()
df = pd.merge(left=df, right=coords, on='address')
df['shape'] = df['shape'].apply(lambda x: lowercase_string(x))


# In[189]:


gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))
gdf.crs = polygons.crs
gdf = gpd.sjoin(gdf, polygons, how='left', op='within')


# In[192]:


# Assuming you have a DataFrame named 'df' with columns 'ZCTA5CE10' and 'shape'

# Group by "ZCTA5CE10" and apply a lambda function to find the mode of "shape"
most_frequent_shape = gdf.groupby('ZCTA5CE10')['shape'].apply(lambda x: mode(x)[0][0] if not x.empty else "N/A")

# Set the index of the result DataFrame
most_frequent_shape.index.name = 'ZCTA5CE10'
most_frequent_shape = most_frequent_shape.reset_index()

# Rename the columns for better clarity
most_frequent_shape.columns = ['ZCTA5CE10', 'most_frequent_shape']
most_frequent_shape.to_csv(f"I:\\Dropbox\\Python\\nuforc\\gis\\csv\\most_frequent_shape.csv")


# In[193]:


random_points = []
# Generate random points within each polygon and add them to the new GeoDataFrame

for idx, row in tqdm(gdf.iterrows()):
    polygon = polygons[polygons['ZCTA5CE10'] == row['ZCTA5CE10']]
    s = gpd.GeoSeries(polygon['geometry'])
    
    random_point = s.sample_points(size=1)
    try:
        row['randomized_x'] = random_point.x.iloc[0]
        row['randomized_y'] = random_point.y.iloc[0]
    except:
        continue
    random_points.append(row)


# In[195]:


randomized_locations_df = pd.DataFrame(random_points)


# In[201]:


randomized_locations_df[['duration', 'occurred_time', 'description', 'shape', 'address', 'ZCTA5CE10', 'latitude', 'longitude', 'randomized_x', 'randomized_y']].to_csv(f"I:\\Dropbox\\Python\\nuforc\\gis\\csv\\randomized_locations.csv")

