import os

from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

"""
Scraper settings.
"""
SCRAPING_MODE = 'timespan'
TIMESPAN_START = "1900-01"
TIMESPAN_END = "2030-01"
N_SCRAPING_RETRIES = 20
OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER")

"""
API keys.
"""
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

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
    },
    "root": {"level": "INFO", "handlers": ["console"]},
}
