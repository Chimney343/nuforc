import logging
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse
from requests.exceptions import ConnectionError, ConnectTimeout, ReadTimeout

logger = logging.getLogger("model.modules.utility")


def get_page(url, n_scraping_retries=10, page_type=''):
    attempt = 0
    while attempt != n_scraping_retries:
        try:
            page = requests.get(url, timeout=5)
            logger.debug(f"{page_type} {url} downloaded.")
            return page
        except (ConnectTimeout, ConnectionError, ReadTimeout) as e:
            attempt += 1
            if attempt != n_scraping_retries:
                logger.warning(
                    f"{page_type} {url} download failed. Retries left: {n_scraping_retries - attempt}. Cause: {e}"
                )
            elif attempt == n_scraping_retries:
                logger.critical(f"{page_type} {url} download failed after max retries.")
                return None


def make_month_root_lookup(n_scraping_retries=10):
    url = "http://www.nuforc.org/webreports/ndxevent.html"
    page = get_page(url=url, n_scraping_retries=n_scraping_retries, page_type='Month page root lookup')
    if page.status_code == 200:
        try:
            soup = BeautifulSoup(page.text, "html.parser")
            monthly_event_summary_tag_list = list(soup.find_all("a", href=True))[1:-1]
            lookup = {}
            for tag in monthly_event_summary_tag_list:
                month = datetime.strptime(tag['href'][4:10], '%Y%m')
                monthly_event_summary_url = urljoin("http://www.nuforc.org/webreports/", tag["href"])
                lookup[month] = monthly_event_summary_url
            return lookup
        except:
            return None
    else:
        raise ConnectionError(
            f"Unable to connect with NUFORC monthly event summary page after {n_scraping_retries} retries."
        )


def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False


def last_day_of_month(date):
    if date.month == 12:
        return date.replace(day=31)
    return date.replace(month=date.month + 1, day=1) - datetime.timedelta(days=1)
