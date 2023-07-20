# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
import sys
from scrapy import Item, Field
from functools import *
from pathlib import Path
sys.path.append(str(Path.cwd().parents[2] / "src"))
from nuforc.wrangling import preprocess_text, extract_time, extract_shape, extract_duration, extract_state, extract_state_abbreviation, extract_country, extract_description


class NuforcEventItem(Item):
    # Primary fields
    occurred_time = Field(input_processor=preprocess_text, output_processor = print)
    # occurred_time = Field(input_processor=preprocess_text, output_processor=partial(extract_time, "occurred_time"))
    # reported_time =
    # entered_as_time =
    # shape =
    # duration =
    # city =
    # state =
    # state_abbreviation =
    # country =
    # description =
    # raw_text =



    # description = Field(input_processor=preprocess_text, output_processor=extract_description)

    # # Calculated fields
    # images = Field()
    # location = Field()
    #
    # # Housekeeping fields
    # url = Field()
    # project = Field()
    # spider = Field()
    # server = Field()
    # date = Field()
