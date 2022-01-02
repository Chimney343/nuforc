import SETTINGS
import logging.config
from model.modules.scraping import NUFORCScraper

logging.config.dictConfig(SETTINGS.LOGGER_CONFIG)
logger = logging.getLogger("root")


def execute_scraping():
    scraper = NUFORCScraper(
        scraping_mode=SETTINGS.SCRAPING_MODE,
        timespan_start=SETTINGS.TIMESPAN_START,
        timespan_end=SETTINGS.TIMESPAN_END,
        n_scraping_retries=SETTINGS.N_SCRAPING_RETRIES,
        output_folder=SETTINGS.OUTPUT_FOLDER,
    )
    scraper.scrape()


if __name__ == "__main__":
    execute_scraping()
