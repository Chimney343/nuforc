#!/usr/bin/env python
# coding: utf-8

# In[3]:


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
import hashlib


# In[39]:


# Events
raw = pd.read_csv(Path(os.getenv('DATA_DIR')) / 'raw_events' / 'events_2023_08_20.csv')
display(raw)


# In[34]:


df = df[['hash', 'city', 'country', 'description', 'duration',
       'entered_as_time', 'occurred_time', 'raw_text', 'reported_time',
       'shape', 'state', 'state_abbreviation']]

df


# In[5]:


def hash_string(s):
    hash_object = hashlib.sha256()
    hash_object.update(s.encode())
    return hash_object.hexdigest()


# In[18]:


# Apply the hashing function to the 'text' column with a progress bar
tqdm.pandas()
df['hash'] = df['raw_text'].progress_apply(lambda x: hash_string(x))


# In[19]:


df.iloc[0]['hash']


# In[35]:


df.to_csv(Path(os.getenv('DATA_DIR')) / 'raw_events' / 'events_2023_07_21.csv', index=False)

