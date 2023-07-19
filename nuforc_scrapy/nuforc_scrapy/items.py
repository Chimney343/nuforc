# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
import sys
from scrapy import Item, Field
from pathlib import Path
sys.path.append(str(Path.cwd().parents[2] / "src"))
from nuforc.wrangling import extract_description


class NuforcEventItem(Item):
    # Primary fields
    description = Field(output_processor=extract_description)

    # # Calculated fields
    # images = Field()
    # location = Field()
    #
    # # Housekeeping fields
    url = Field()
    # project = Field()
    # spider = Field()
    # server = Field()
    # date = Field()
