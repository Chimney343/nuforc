import concurrent.futures
import logging
import pickle
from collections import OrderedDict
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin

import validators
from bs4 import BeautifulSoup
from dateutil.parser import parse
from dateutil.relativedelta import *
from tqdm.autonotebook import tqdm

from src.nuforc.utility import (
    get_page,
    is_date,
    last_day_of_month,
    make_month_root_lookup,
)
from src.nuforc.wrangling import RawEventProcessor, parse_time

logger = logging.getLogger("model.modules.scraping")

# TODO: Whole `scraping` module is now obsolete due to adoption of scrapy for scraping.
class NUFORCScraper:
    available_scraping_modes = ["full", "timespan"]

    def __init__(
        self,
        scraping_mode="full",
        timespan_start=None,
        timespan_end=None,
        n_scraping_retries=10,
        output_folder="output",
    ):
        # Setting up lookups.
        self.month_to_url_lookup = make_month_root_lookup(
            n_scraping_retries=n_scraping_retries
        )

        self.url_to_month_lookup = {
            url: month for month, url in self.month_to_url_lookup.items()
        }

        # Setting up scraping mode.
        assert (
            scraping_mode in self.available_scraping_modes
        ), f"Invalid scraping mode chosen; available scraping modes are: {self.available_scraping_modes}"
        self.scraping_mode = scraping_mode

        if self.scraping_mode == "full":
            self.timespan_start = min(self.month_to_url_lookup.keys())
            self.timespan_end = last_day_of_month(max(self.month_to_url_lookup.keys()))

        if self.scraping_mode == "timespan":
            self.timespan_start, self.timespan_end = self._validate_timespan_boundaries(
                timespan_start=timespan_start, timespan_end=timespan_end
            )

        self.timespan_in_months = self._calculate_timespan_in_months()
        self.n_scraping_retries = n_scraping_retries
        self.output_folder = None
        self.parsed_month_root_pages = None
        self.event_lookup = None
        self.month_root_urls_to_scrape = None
        self.output_folder = output_folder

    def _validate_timespan_boundaries(self, timespan_start, timespan_end):
        if not is_date(timespan_start):
            raise ValueError(f"{timespan_start} is not a valid date format.")
        if not is_date(timespan_end):
            raise ValueError(f"{timespan_end} is not a valid date format.")

        timespan_start = parse(timespan_start)
        timespan_end = parse(timespan_end)

        if timespan_start > timespan_end:
            raise ValueError(
                f"Timespan start can't be higher than timespan_in_months start: {timespan_start} > {timespan_end}"
            )
        return timespan_start, timespan_end

    def _cast_datestring_to_datetime_year_and_month(self, date):
        """
        Parse a datetime-like string and return a datetime object containing only year and month of a datetime.
        :param date:
        :return:
        """
        logger.info(date, type(date))
        date = parse(date)
        date = date.strftime("%Y-%m")
        date = datetime.strptime(date, "%Y-%m")
        return date

    def _calculate_timespan_in_months(self):
        """
        Parses timespan_in_months start month and timespan_in_months end month and calculates months in between.
        :return:
        """
        timespan_start_month = self.timespan_start
        # Add one month to timespan_in_months end so the calculated range 'catches' the original final month.
        timespan_end_month = self.timespan_end + relativedelta(months=+1)

        return list(
            OrderedDict(
                ((timespan_start_month + timedelta(_)).replace(day=1), None)
                for _ in range((timespan_end_month - timespan_start_month).days)
            ).keys()
        )

    def select_month_root_urls_to_scrape(self, timespan_in_months=None):
        if timespan_in_months is None:
            timespan_in_months = self.timespan_in_months
        urls = [self.month_to_url_lookup.get(month) for month in timespan_in_months]
        urls = [url for url in urls if url is not None]
        return urls

    def _get_month_root_page_response(self, month_root_url, n_scraping_retries):
        return get_page(
            url=month_root_url,
            n_scraping_retries=n_scraping_retries,
            page_label="Month root page",
        )

    def parse_month_root_page(self, url, n_scraping_retries):
        response = self._get_month_root_page_response(
            month_root_url=url, n_scraping_retries=n_scraping_retries
        )
        if response and response.status_code == 200:
            return BeautifulSoup(response.text, "html.parser")

    def parse_month_root_pages(self, month_root_pages, n_scraping_retries):
        futures = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            future_to_url = {
                executor.submit(
                    self.parse_month_root_page, url, n_scraping_retries
                ): url
                for url in month_root_pages
            }
            for future in tqdm(
                concurrent.futures.as_completed(future_to_url),
                total=len(month_root_pages),
                desc="Sifting through month root pages. ",
            ):
                url = future_to_url[future]
                try:
                    data = future.result()
                    futures[url] = data
                except:
                    logger.critical(
                        f"Month root page at {url} returned an unhandled exception during scraping attempt."
                    )
                    futures[url] = None
        return futures

    def get_event_url_from_parsed_month_root_page(self, parsed_month_root_page):
        event_tags = parsed_month_root_page.find_all("a", href=True)[1:]
        events = {
            parse_time(tag.text): urljoin(
                "http://www.nuforc.org/webreports/", tag["href"]
            )
            for tag in event_tags
        }

        return events

    def _make_event_url_lookup(self, parsed_month_root_pages):
        event_lookup = {}
        for page in tqdm(
            parsed_month_root_pages.values(),
            total=len(parsed_month_root_pages),
            desc="Reading event dates. ",
        ):
            page_lookup = self.get_event_url_from_parsed_month_root_page(page)
            event_lookup.update(page_lookup)

        if self.scraping_mode == "timespan":
            filtered_event_lookup = {}
            for event_date, event_url in event_lookup.items():
                if self.timespan_start <= event_date <= self.timespan_end:
                    filtered_event_lookup[event_date] = event_url
            return filtered_event_lookup
        return event_lookup

    def scrape_event(self, event_url, n_scraping_retries):
        event_scraper = EventScraper(
            report_url=event_url, n_scraping_retries=n_scraping_retries
        )
        event_scraper.scrape()
        return event_scraper.event

    def _scrape_multiple_events(self, event_urls):
        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            event_to_url = {
                executor.submit(
                    self.scrape_event, event_url, self.n_scraping_retries
                ): event_url
                for event_url in event_urls
            }
            for future in tqdm(
                concurrent.futures.as_completed(event_to_url),
                total=len(event_urls),
                desc="Reading events. ",
            ):
                event_url = event_to_url[future]
                try:
                    event = future.result()
                    futures.append(event)
                except Exception as e:
                    logger.critical(
                        f"NUFORC event at {event_url} returned an unhandled exception during scraping attempt. {e}"
                    )
        return futures

    def scrape(self):
        if self.scraping_mode == "full":
            self.month_root_urls_to_scrape = self.month_to_url_lookup.values()
        elif self.scraping_mode == "timespan":
            self.month_root_urls_to_scrape = self.select_month_root_urls_to_scrape()

        self.parsed_month_root_pages = self.parse_month_root_pages(
            month_root_pages=self.month_root_urls_to_scrape,
            n_scraping_retries=self.n_scraping_retries,
        )
        self.event_lookup = self._make_event_url_lookup(
            parsed_month_root_pages=self.parsed_month_root_pages
        )
        logger.info(
            f"Scraping events from {min(self.event_lookup)} to {max(self.event_lookup)}."
        )
        self.events = self._scrape_multiple_events(
            event_urls=self.event_lookup.values()
        )

    def save_events(self):
        path = Path(self.output_folder)
        path.mkdir(parents=True, exist_ok=True)
        date_today = date.today().strftime("%Y_%m_%d")

        filename_path = path / f"events_{date_today}.pkl"
        metadata_filename = path / f"events_metadata_{date_today}.pkl"
        with open(filename_path, "wb") as f:
            pickle.dump(self.events, f)
            logger.info(f"Events saved @ {filename_path}")
        # with open(metadata_filename, "wb") as f:
        #     pickle.dump(self.events_metadata, f)


class EventScraper:
    def __init__(self, report_url, n_scraping_retries=10):
        assert validators.url(report_url), f"{report_url} is not a valid URL."
        self.report_url = report_url
        self.n_scraping_retries = n_scraping_retries
        self.status = "unprocessed"
        self.page = None
        self.start_time = None
        self.end_time = None
        self.duration = None
        self.status_code = None

    def _get_event_page(self):
        self.page = get_page(
            url=self.report_url,
            n_scraping_retries=self.n_scraping_retries,
            page_label="Event page",
        )
        self.status_code = self.page.status_code

    def _parse_event_page(self):
        soup = BeautifulSoup(self.page.text, "html.parser")
        raw_report = "".join([tag.text for tag in soup.find_all("tr")])
        return raw_report

    def _get_raw_event(self):
        """
        Downloads raw report from URL submitted to __init__ and parses according to page status code and page content.
        """
        self._get_event_page()
        if self.status_code == 200:
            if self.page.text == "":
                self.raw_event = "Blank report"
            else:
                self.raw_event = self._parse_event_page()
        else:
            self.raw_event = "Unable to download report"

    def _process_event(self):
        """
        Processes the report submitted to __init__; either downloaded from report URL or submitted report text.
        """
        self.start_time = datetime.now()

        # Event processing starts here.
        self._get_raw_event()
        raw_event_processor = RawEventProcessor(
            raw_event=self.raw_event, report_url=self.report_url
        )
        event = raw_event_processor.read_event()
        # Event processing ends here.

        self.status = "processed"
        self.end_time = datetime.now()
        self.duration = self.end_time - self.start_time
        return event

    def scrape(self):
        self.event = self._process_event()
        logger.debug((f"Report @ {self.report_url} scraped."))
