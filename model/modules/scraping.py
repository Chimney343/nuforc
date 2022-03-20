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

from model.modules.utility import (get_page, is_date, last_day_of_month,
                                   make_month_root_lookup)
from model.modules.wrangling import NUFORCReportProcessor, parse_time

logger = logging.getLogger("model.modules.scraping")


class NUFORCScraper:
    available_scraping_modes = ["full", "timespan"]
    timespan_start = None
    timespan_end = None
    output_folder = None
    parsed_month_root_pages = None
    event_lookup = None
    month_root_urls_to_scrape = None

    def __init__(
        self,
        scraping_mode="full",
        timespan_start=None,
        timespan_end=None,
        n_scraping_retries=10,
        output_folder='output',
    ):
        # Setting up lookups.
        self.month_to_url_lookup = make_month_root_lookup(n_scraping_retries=n_scraping_retries)
        assert (
            self.month_to_url_lookup
        ), f"Unable to connect with NUFORC monthly event summary page after {n_scraping_retries} retries."
        self.url_to_month_lookup = {url: month for month, url in self.month_to_url_lookup.items()}

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

        self.timespan = self._calculate_timespan()
        self.n_scraping_retries = n_scraping_retries
        self.output_folder = output_folder

    def _validate_timespan_boundaries(self, timespan_start, timespan_end):
        if not is_date(timespan_start):
            raise ValueError(f"{timespan_start} is not a valid date format.")
        if not is_date(timespan_end):
            raise ValueError(f"{timespan_end} is not a valid date format.")

        if timespan_start > timespan_end:
            raise ValueError(f"Timespan start can't be higher than timespan start: {timespan_start} > {timespan_end}")
        return timespan_start, timespan_end

    def _cast_datestring_to_datetime_year_and_month(self, date):
        """
        Parse a datetime-like string and return a datetime object containing only year and month of a datetime.
        :param date:
        :return:
        """
        date = parse(date)
        date = date.strftime("%Y-%m")
        date = datetime.strptime(date, "%Y-%m")
        return date

    def _calculate_timespan(self):
        """
        Parses timespan start month and timespan end month and calculates months in between.
        :return:
        """
        timespan_start_month = self._cast_datestring_to_datetime_year_and_month(self.timespan_start)
        # Add one month to timespan end so the calculated range 'catches' the original final month.
        timespan_end_month = self._cast_datestring_to_datetime_year_and_month(self.timespan_end) + relativedelta(
            months=+1
        )

        return list(
            OrderedDict(
                ((timespan_start_month + timedelta(_)).strftime(r"%Y-%m"), None)
                for _ in range((timespan_end_month - timespan_start_month).days)
            ).keys()
        )

    def _select_month_root_urls_to_scrape(self):
        timespan = [self._cast_datestring_to_datetime_year_and_month(month) for month in self.timespan]
        urls = [self.month_to_url_lookup.get(month) for month in timespan]
        urls = [url for url in urls if url is not None]
        return urls

    def _get_month_root_page_response(self, month_root_url, n_scraping_retries):
        return get_page(
            url=month_root_url,
            n_scraping_retries=n_scraping_retries,
            page_type="Month root page",
        )

    def parse_month_root_page(self, url, n_scraping_retries):
        response = self._get_month_root_page_response(month_root_url=url, n_scraping_retries=n_scraping_retries)
        if response and response.status_code == 200:
            return BeautifulSoup(response.text, "html.parser")

    def _parse_month_root_pages(self, month_root_pages, n_scraping_retries):
        futures = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            future_to_url = {
                executor.submit(self.parse_month_root_page, url, n_scraping_retries): url for url in month_root_pages
            }
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                logger.info((f"Month root page @ {url} scraped."))
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
            parse_time(tag.text): urljoin("http://www.nuforc.org/webreports/", tag["href"]) for tag in event_tags
        }

        return events

    def _make_event_url_lookup(self, parsed_month_root_pages):
        event_lookup = {}
        for page in parsed_month_root_pages.values():
            page_lookup = self.get_event_url_from_parsed_month_root_page(page)
            event_lookup.update(page_lookup)

        if self.scraping_mode == 'timespan':
            timespan_end = parse_time(self.timespan_end)
            event_lookup = {
                event_date: event_url for event_date, event_url in event_lookup.items() if event_date <= timespan_end
            }

        return event_lookup

    def scrape_event(self, event_url, n_scraping_retries):
        event = NUFORCReport(report_url=event_url, n_scraping_retries=n_scraping_retries)
        event.scrape()
        return event

    def _scrape_multiple_events(self, event_urls):
        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            event_to_url = {
                executor.submit(self.scrape_event, event_url, self.n_scraping_retries): event_url
                for event_url in self.event_lookup.values()
            }
            for future in concurrent.futures.as_completed(event_to_url):
                event_url = event_to_url[future]
                try:
                    event = future.result()
                    futures.append(event.data)
                except:
                    logger.critical(
                        f"NUFORC event at {event_url} returned an unhandled exception during scraping attempt."
                    )
        return futures

    def scrape(self):
        if self.scraping_mode == 'full':
            self.month_root_urls_to_scrape = self.month_to_url_lookup.values()
        elif self.scraping_mode == 'timespan':
            self.month_root_urls_to_scrape = self._select_month_root_urls_to_scrape()

        self.parsed_month_root_pages = self._parse_month_root_pages(
            month_root_pages=self.month_root_urls_to_scrape, n_scraping_retries=self.n_scraping_retries
        )
        self.event_lookup = self._make_event_url_lookup(parsed_month_root_pages=self.parsed_month_root_pages)
        self.events = self._scrape_multiple_events(event_urls=self.event_lookup.values())

    def save_events(self):
        path = Path(self.output_folder)
        path.mkdir(parents=True, exist_ok=True)
        date_today = date.today().strftime('%Y_%m_%d')

        filename_path = path / f"events_{date_today}.pkl"
        metadata_filename = path / f"events_metadata_{date_today}.pkl"
        with open(filename_path, "wb") as f:
            pickle.dump(self.events, f)
        # with open(metadata_filename, "wb") as f:
        #     pickle.dump(self.events_metadata, f)


class NUFORCReport:
    def __init__(self, report_url=None, raw_report=None, n_scraping_retries=5):
        assert report_url is not None or raw_report is not None, "Provide either report URL or raw report text."
        if report_url:
            assert validators.url(report_url), f"{report_url} is not a valid URL."
            assert raw_report is None, "Provide either report URL or raw report text; cannot provide both."

        self.report_url = report_url
        self.raw_report = raw_report
        self.n_scraping_retries = n_scraping_retries
        self.metadata = {
            "url": self.report_url,
            "status": "unprocessed",
            "start_time": None,
            "end_time": None,
            "duration": None,
            "page_status_code": None,
        }

    def _get_report_page(self):
        self.page = get_page(
            url=self.report_url,
            n_scraping_retries=self.n_scraping_retries,
            page_type="Report",
        )
        self.metadata['page_status_code'] = self.page.status_code

    def _parse_report_page(self):
        soup = BeautifulSoup(self.page.text, "html.parser")
        raw_report = "".join([tag.text for tag in soup.find_all("tr")])
        return raw_report

    def _get_raw_report(self):
        """
        Downloads raw report from URL submitted to __init__ and parses according to page status code and page content.
        """
        self._get_report_page()
        if self.metadata['page_status_code'] == 200:
            if self.page.text == '':
                self.raw_report = "Blank report"
            else:
                self.raw_report = self._parse_report_page()
        else:
            self.raw_report = "Unable to download report"

    def _process_report(self):
        """
        Processes the report submitted to __init__; either downloaded from report URL or submitted report text.
        """
        if not self.raw_report:
            self._get_raw_report()
        report_processor = NUFORCReportProcessor(raw_report=self.raw_report, report_url=self.report_url)
        self.data = report_processor.get_report()

    def scrape(self):
        start_time = datetime.now()
        self._process_report()
        end_time = datetime.now()
        self.metadata.update(
            {
                "status": "processed",
                "start_time": start_time,
                "end_time": end_time,
                "duration": end_time - start_time,
            }
        )

        if self.report_url:
            logger.info((f"Report @ {self.report_url} scraped."))
        else:
            logger.info(f"Raw report scraped.")
