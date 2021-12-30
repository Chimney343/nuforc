"""
Scraper settings.
"""
SCRAPING_MODE = 'full'
TIMESPAN_START = "2021-01-01"
TIMESPAN_END = "2021-01-10"
N_SCRAPING_RETRIES = 20
OUTPUT_FOLDER = 'data'

"""
Logger settings.
"""

LOGGER_CONFIG = {
    "version": 1,
    "formatters": {
        "simple": {"format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"}
    },
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
