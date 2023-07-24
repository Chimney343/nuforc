import scrapy
from scrapy.loader import ItemLoader
from urllib.parse import urljoin
from nuforc_scrapy.items import NuforcEventItem

import sys
from pathlib import Path

sys.path.append(str(Path.cwd().parents[2] / "src"))
from nuforc.wrangling import extract_description, preprocess_text


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
        l = ItemLoader(item=NuforcEventItem(), response=response)
        l.add_xpath("occurred_time", "//body//text()")
        l.add_xpath("reported_time", "//body//text()")
        l.add_xpath("entered_as_time", "//body//text()")
        l.add_xpath("shape", "//body//text()")
        l.add_xpath("duration", "//body//text()")
        l.add_xpath("city", "//body//text()")
        l.add_xpath("state", "//body//text()")
        l.add_xpath("state_abbreviation", "//body//text()")
        l.add_xpath("country", "//body//text()")
        l.add_xpath("description", "//body//text()")
        l.add_xpath("raw_text", "//body//text()")

        # l.add_xpath('description', "//body//text()")
        item = l.load_item()

        yield item
