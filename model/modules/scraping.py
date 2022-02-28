import concurrent.futures
from urllib.parse import urljoin
import pickle
import logging
from pathlib import Path
from bs4 import BeautifulSoup
from model.modules.wrangling import NUFORCReportProcessor
import validators
from model.modules.utility import (
    get_page,
    make_monthly_event_summary_lookup,
    validate_datetime_format,
    last_day_of_month,
)
from datetime import date
from datetime import datetime, timedelta
from collections import OrderedDict
from dateutil.relativedelta import *

import logging
logger = logging.getLogger("model.modules.scraping")

def make_and_scrape_nuforc_event(event_url, n_scraping_retries):
    event = NUFORCReport(report_url=event_url, n_scraping_retries=n_scraping_retries)
    event.scrape()
    return event

def download_multiple_nuforc_events(event_urls, n_scraping_retries=10):
    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        future_to_url = {
            executor.submit(make_and_scrape_nuforc_event, url, n_scraping_retries): url
            for url in event_urls
        }
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
                futures.append(data)
            except:
                logger.critical(
                    f"Event at {url} returned an unhandled exception during multiple scraping attempt."
                )
                futures.append(data)
    return futures


def make_monthly_summary_event_dates_lookup(summary_url, n_scraping_retries=10):
    summary = NUFORCMonthlyEventsSummary(
        summary_url=summary_url, n_scraping_retries=n_scraping_retries
    )
    summary.make_event_dates_lookup()
    return summary.events_dates_lookup


def make_multiple_monthly_summary_event_dates_lookups(
    summary_urls, n_scraping_retries=10
):
    futures = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        future_to_url = {
            executor.submit(
                make_monthly_summary_event_dates_lookup, url, n_scraping_retries
            ): url
            for url in summary_urls
        }
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
                futures.update(data)
            except:
                logger.critical(
                    f"Event at {url} returned an unhandled exception during multiple scraping attempt."
                )
                futures.update(data)
    return futures


class NUFORCScraper:
    available_scraping_modes = ["full", "timespan"]
    timespan_start = None
    timespan_end = None
    events = []
    events_metadata = []
    output_folder = None

    def __init__(
        self,
        scraping_mode="full",
        timespan_start=None,
        timespan_end=None,
        n_scraping_retries=10,
        output_folder='output'
    ):

        if scraping_mode not in self.available_scraping_modes:
            raise ValueError(
                f"invalid scraping mode chosen; available scraping modes are: {self.available_scraping_modes}"
            )
        else:
            self.scraping_mode = scraping_mode
        self.month_to_url_lookup = make_monthly_event_summary_lookup(
            n_scraping_retries=n_scraping_retries
        )
        self.url_to_month_lookup = {
            url: month for month, url in self.month_to_url_lookup.items()
        }

        if self.scraping_mode == "full":
            self.timespan_start = min(self.month_to_url_lookup.keys())
            self.timespan_end = last_day_of_month(max(self.month_to_url_lookup.keys()))

        if self.scraping_mode == "timespan":
            self.timespan_start = self.validate_timespan_boundaries(
                timespan_start=timespan_start,
                timespan_end=timespan_end,
            )[0]
            self.timespan_end = self.validate_timespan_boundaries(
                timespan_start=timespan_start,
                timespan_end=timespan_end,
            )[1]

        self.timespan = self.calculate_timespan()
        self.timespan_months = self.calculate_timespan_months()
        self.n_scraping_retries = n_scraping_retries
        self.output_folder = output_folder

    def validate_timespan_boundaries(self, timespan_start, timespan_end):
        try:
            timespan_start = validate_datetime_format(timespan_start)
            timespan_end = validate_datetime_format(timespan_end)
        except ValueError:
            raise ValueError(f"Either timespan start or timespan end are wrong.")

        if timespan_start > timespan_end:
            raise ValueError(
                f"Timespan start can't be higher than timespan start: {timespan_start} > {timespan_end}"
            )
        return timespan_start, timespan_end

    def calculate_timespan(self):
        timespan_start_month = self.timespan_start.strftime("%Y-%m")
        timespan_start_month = datetime.strptime(timespan_start_month, "%Y-%m")

        # Add one month to timespan end so the calculated range 'catches' the original final month.
        timespan_end_month = self.timespan_end + relativedelta(months=+1)
        timespan_end_month = datetime.strptime(
            timespan_end_month.strftime("%Y-%m"), "%Y-%m"
        )

        return list(
            OrderedDict(
                ((timespan_start_month + timedelta(_)).strftime(r"%Y-%m"), None)
                for _ in range((timespan_end_month - timespan_start_month).days)
            ).keys()
        )

    def calculate_timespan_months(self):
        timespan_in_months = [
            datetime.strptime(month, "%Y-%m") for month in self.timespan
        ]
        timespan_months = {
            month: url
            for month, url in self.month_to_url_lookup.items()
            if month in timespan_in_months
        }
        return timespan_months

    def make_event_lookup(self):
        timespan_in_months = [
            datetime.strptime(month, "%Y-%m") for month in self.timespan
        ]
        timespan_months_summaries_lookup = {
            month: url
            for month, url in self.month_to_url_lookup.items()
            if month in timespan_in_months
        }
        events_dates_lookup = make_multiple_monthly_summary_event_dates_lookups(
            summary_urls=timespan_months_summaries_lookup.values(),
            n_scraping_retries=self.n_scraping_retries,
        )

        return events_dates_lookup

    def save_output(self):
        if self.events == [] or self.events_metadata == []:
            raise AttributeError(f"There are no events to save.")

        path = Path(self.output_folder)
        path.mkdir(parents=True, exist_ok=True)
        date_today = date.today().strftime('%Y_%m_%d')

        filename_path = path / f"events_{date_today}.pkl"
        metadata_filename = path / f"events_metadata_{date_today}.pkl"
        with open(filename_path, "wb") as f:
            pickle.dump(self.events, f)
        with open(metadata_filename, "wb") as f:
            pickle.dump(self.events_metadata, f)

    def scrape(self):
        logger.info(f"Scraper initialized.\nCreating event dates lookup...")
        self.events_dates_lookup = self.make_event_lookup()
        logger.info(f"Event dates lookup created. Downloading {len(self.events_dates_lookup)} events.")
        events = download_multiple_nuforc_events(
            event_urls=self.events_dates_lookup.values(),
            n_scraping_retries=self.n_scraping_retries,
        )
        self.events = [event.event_summary for event in events]
        self.events_metadata = [event.metadata for event in events]
        self.save_output()
        logger.info(f"Scraping finished.")


class NUFORCMonthlyEventsSummary:
    events_dates_lookup = {}
    events = {}
    events_metadata = {}
    metadata = {
        "status": "not scraped",
        "start time": None,
        "end time": None,
        "duration": None,
    }

    def __init__(self, summary_url, n_scraping_retries=30):
        self.summary_url = summary_url
        self.n_scraping_retries = n_scraping_retries

    def __get_summary_page(self):
        return get_page(
            url=self.summary_url,
            n_scraping_retries=self.n_scraping_retries,
            page_type="Monthly summary",
        )

    def make_event_dates_lookup(self):
        page = self.__get_summary_page()
        soup = BeautifulSoup(page.text, "html.parser")
        event_tags = soup.find_all("a", href=True)[1:]
        events = {
            parse_time(tag.text): urljoin(
                "http://www.nuforc.org/webreports/", tag["href"]
            )
            for tag in event_tags
        }
        self.events_dates_lookup = events

    def __make_and_scrape_nuforc_event(self, event_url):
        event = NUFORCReport(
            report_url=event_url, n_scraping_retries=self.n_scraping_retries
        )
        event.scrape()
        return event

    def __download_multiple_nuforc_events(self, event_urls):
        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            future_to_url = {
                executor.submit(self.__make_and_scrape_nuforc_event, url): url
                for url in event_urls
            }
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    data = future.result()
                    futures.append(data)
                except:
                    logger.critical(
                        f"Event at {url} returned an unhandled exception during multiple scraping attempt."
                    )
                    futures.append(data)
        return futures

    def save_output(self):
        pass

    def scrape(self):
        start_time = datetime.now()
        self.make_event_dates_lookup()
        if not self.events_dates_lookup:
            # Scraping failed; update metadata.
            end_time = datetime.now()
            self.metadata.update(
                {
                    "status": "failed",
                    "start time": start_time,
                    "end time": end_time,
                    "duration": end_time - start_time,
                }
            )
            logger.critical(
                f"\n{self.month} scraping failed; couldn't get monthly summary page.'\n---"
            )
        else:
            # Download and read events.
            events = self.__download_multiple_nuforc_events(
                event_urls=self.events_dates_lookup.values()
            )
            self.events = [event.event_summary for event in events]
            self.events_metadata = [event.metadata for event in events]
            # Update metadata.
            end_time = datetime.now()
            self.metadata.update(
                {
                    "status": "success",
                    "start time": start_time,
                    "end time": end_time,
                    "duration": end_time - start_time,
                }
            )
            logger.info(
                f"\n{self.summary_url} events: {len(self.events)}\nScraping time: {self.metadata.get('duration')}\n---"
            )


class NUFORCReport:
    def __init__(self, report_url=None, raw_report=None, n_scraping_retries=5):
        assert report_url is not None or raw_report is not None, "Provide either report URL or raw report teRext."
        if report_url:
            assert validators.url(report_url), f"{report_url} is not a valid URL."
            assert raw_report is None, "Provide either report URL or raw report text; cannot provide both."

        self.report_url = report_url
        self.raw_report = raw_report
        self.n_scraping_retries = n_scraping_retries
        self.metadata = {
            "url": self.report_url,
            "status": "not scraped",
            "start time": None,
            "end time": None,
            "duration": None,
        }

    def _get_report_page(self):
        self.page = get_page(
            url=self.report_url,
            n_scraping_retries=self.n_scraping_retries,
            page_type="Report",
        )
        self.page_status_code = self.page.status_code

    def _get_raw_report(self):
        self._get_report_page()
        if self.page.status_code == 200 and self.page.text == '':
            self.raw_report = self.report_url + "\n" + "Blank report"
        elif self.page.status_code == 200:
            soup = BeautifulSoup(self.page.text, "html.parser")
            raw_report = "".join(
            [tag.text for tag in soup.find_all("tr")])
            self.raw_report = self.report_url + "\n" + raw_report
        else:
            self.raw_report = self.report_url + "\n" + "Unable to download report"

    def _process_report(self):
        if not self.raw_report:
            self._get_raw_report()
        report_processor = NUFORCReportProcessor(raw_report=self.raw_report)
        self.info = report_processor.get_report()

    def scrape(self):
        start_time = datetime.now()
        self._process_report()
        end_time = datetime.now()
        self.metadata.update(
            {
                "status": "success",
                "start time": start_time,
                "end time": end_time,
                "duration": end_time - start_time,
            }
        )

        if self.report_url:
            logger.info((f"Report @ {self.report_url} scraped."))
        else:
            logger.info(f"Raw report scraped.")