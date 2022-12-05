from os import environ
from dataclasses import dataclass
from datetime import date
from datetime import timedelta

from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

@dataclass(frozen=True)
class ScraperSettings:
    scraping_mode: str = 'timespan'
    timespan_start: str = date.today().strftime("%Y/%m/%d")
    timespan_end = (date.today() - timedelta(days=30)).strftime("%Y/%m/%d")
    n_scraping_retries: int = 30
    output_folder = environ.get("OUTPUT_FOLDER") or 'data'

DEFAULT_ENGINE_SETTINGS = ScraperSettings()
"""
Scraper settings.
"""
SCRAPING_MODE = 'timespan'
TIMESPAN_START = "2022-6-10"
TIMESPAN_END = "2022-06-20"
N_SCRAPING_RETRIES = 20
OUTPUT_FOLDER = environ.get("OUTPUT_FOLDER")

"""
Geocoder settings.
"""


"""
Dashboard settings.
"""
DASHBOARD_APP_DIR = 'dashboard'

"""
API keys.
"""
# Google Geocoding API key.
API_KEY = environ.get("API_KEY")

"""
Logger settings.
"""

LOGGING_CONFIG = {
    "version": 1,
    "formatters": {"simple": {"format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"}},
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "simple",
            "stream": "ext://sys.stdout",
        }
    },
    "loggers": {
        "model.modules.wrangling": {
            "level": "INFO",
            "handlers": ["console"],
            "formatter": "simple",
            "propagate": False,
        },
        "model.modules.scraping": {
            "level": "INFO",
            "handlers": ["console"],
            "formatter": "simple",
            "propagate": False,
        },
        "model.modules.utility": {
            "level": "INFO",
            "handlers": ["console"],
            "formatter": "simple",
            "propagate": False,
        },
        "model.modules.geocoding": {
            "level": "INFO",
            "handlers": ["console"],
            "formatter": "simple",
            "propagate": False,
        },
    },
    "root": {"level": "INFO", "handlers": ["console"]},
}
