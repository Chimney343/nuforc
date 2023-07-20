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
            url = urljoin('https://nuforc.org/webreports/', url)
            yield response.follow(url, self.parse_subpage)

    def parse_subpage(self, response):
        for url in response.css("a::attr(href)").getall():
            url = urljoin('https://nuforc.org/webreports/', url)
            yield response.follow(url, self.parse_event_page)

    def parse_event_page(self, response):
        l = ItemLoader(item=NuforcEventItem(), response=response)
        l.add_xpath('occurred_time', "//body//text()")
        # l.add_xpath('description', "//body//text()")
        item = l.load_item()

        yield item
