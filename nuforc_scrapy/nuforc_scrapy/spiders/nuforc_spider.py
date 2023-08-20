import os
import sys
from pathlib import Path
from urllib.parse import urljoin

import scrapy
from dotenv import load_dotenv
from scrapy.loader import ItemLoader

from nuforc_scrapy.items import NuforcEventItem

sys.path.append(str(Path.cwd().parents[2] / "src"))
load_dotenv()


class NuforcSpider(scrapy.Spider):
    name = "nuforc_spider"
    start_urls = ["https://nuforc.org/webreports/ndxevent.html"]

    def parse(self, response):
        for url in response.css("a::attr(href)").getall():
            url = urljoin("https://nuforc.org/webreports/", url)
            yield response.follow(url, self.parse_subpage)

    def parse_subpage(self, response):
        for url in response.css("a::attr(href)").getall():
            url = urljoin("https://nuforc.org/webreports/", url)
            yield response.follow(url, self.parse_event_page)

    def parse_event_page(self, response):
        loader = ItemLoader(item=NuforcEventItem(), response=response)

        # Primary fields.
        loader.add_xpath("occurred_time", "//body//text()")
        loader.add_xpath("reported_time", "//body//text()")
        loader.add_xpath("entered_as_time", "//body//text()")
        loader.add_xpath("shape", "//body//text()")
        loader.add_xpath("duration", "//body//text()")
        loader.add_xpath("city", "//body//text()")
        loader.add_xpath("state", "//body//text()")
        loader.add_xpath("state_abbreviation", "//body//text()")
        loader.add_xpath("country", "//body//text()")
        loader.add_xpath("description", "//body//text()")
        loader.add_xpath("raw_text", "//body//text()")
        item = loader.load_item()

        # Calculated fields.
        item.set_hash_field()
        item.set_address_field()

        yield item
