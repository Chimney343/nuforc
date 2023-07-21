import logging.config
import pickle
from pathlib import Path

import pandas as pd

from src.nuforc import SETTINGS
from src.nuforc.geocoding import NUFORCGeocoder

logging.config.dictConfig(SETTINGS.LOGGING_CONFIG)
logger = logging.getLogger("root")


def execute_geocoding(events_filepath, geocoding_cache_filepath=None):
    events_filepath = Path(events_filepath)
    with open(events_filepath, "rb") as pickle_file:
        data = pickle.load(pickle_file)
    df = pd.DataFrame(data)
    geocoder = NUFORCGeocoder(input=df)
    geocoder.run()


if __name__ == "__main__":
    execute_geocoding(
        events_filepath=Path(SETTINGS.OUTPUT_FOLDER) / "events_2022_08_16.pkl"
    )
