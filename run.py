import logging.config

from src.nuforc.SETTINGS import DEFAULT_ENGINE_SETTINGS
from src.nuforc.SETTINGS import LOGGING_CONFIG
from src.nuforc.scraping import NUFORCScraper

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger("root")


def execute_scraping():
    scraper = NUFORCScraper(
        scraping_mode=DEFAULT_ENGINE_SETTINGS.scraping_mode,
        timespan_start=DEFAULT_ENGINE_SETTINGS.timespan_start,
        timespan_end=DEFAULT_ENGINE_SETTINGS.timespan_end,
        n_scraping_retries=DEFAULT_ENGINE_SETTINGS.n_scraping_retries,
        output_folder=DEFAULT_ENGINE_SETTINGS.output_folder,
    )
    scraper.scrape()
    scraper.save_events()


if __name__ == "__main__":
    execute_scraping()
