#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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

from src.nuforc import SETTINGS
from src.nuforc.utility import *
from src.nuforc.geocoding.geocoder import Geolocator
from src.nuforc.geocoding.wrangling import replace_unparsed_with_none, join_columns
from datetime import date


# In[ ]:


df = pd.read_csv('nuforc.csv')
df = replace_unparsed_with_none(df)
df['address'] = df.apply(lambda row: join_columns(row['city'], row['state'], row['country']), axis=1)
display(df)


# In[ ]:


# Configure logging
logging.basicConfig(filename="geolocation.log", level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

geolocator = Geolocator()
geolocator.geolocate_addresses(df['address'].unique())

output_csv_file = "geolocation_results.csv"
geolocator.write_to_csv(output_csv_file)

logging.info("Geolocation process completed and results written to 'geolocation_results.csv'.")

