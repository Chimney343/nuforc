# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
import sys
from scrapy import Item, Field
from functools import *
from pathlib import Path

sys.path.append(str(Path.cwd().parents[2] / "src"))
from nuforc.wrangling import (
    preprocess_text,
    extract_time,
    extract_shape,
    extract_duration,
    extract_state,
    extract_state_abbreviation,
    extract_country,
    extract_description,
    extract_city,
)


def pick_first(iterable):
    """
    Custom output processor to always pick the first item from the iterable.
    If the iterable is empty, it returns None.
    """
    if iterable:
        return next(iter(iterable))
    return None


def final_processing(iterable, wrapping_func):
    """
    Custom function to process the output of pick_first.
    Accepts a wrapping function to apply additional processing.
    """
    selected_value = pick_first(iterable)
    if selected_value is not None:
        return wrapping_func(selected_value)
    return None


class NuforcEventItem(Item):
    # Primary fields
    occurred_time = Field(
        input_processor=preprocess_text,
        output_processor=lambda x: final_processing(
            x, partial(extract_time, time_type="occurred_time")
        ),
    )
    reported_time = Field(
        input_processor=preprocess_text,
        output_processor=lambda x: final_processing(
            x, partial(extract_time, time_type="reported_time")
        ),
    )
    entered_as_time = Field(
        input_processor=preprocess_text,
        output_processor=lambda x: final_processing(
            x, partial(extract_time, time_type="entered_as_time")
        ),
    )
    shape = Field(
        input_processor=preprocess_text,
        output_processor=lambda x: final_processing(x, extract_shape),
    )
    duration = Field(
        input_processor=preprocess_text,
        output_processor=lambda x: final_processing(x, extract_duration),
    )
    city = Field(
        input_processor=preprocess_text,
        output_processor=lambda x: final_processing(x, extract_city),
    )
    state = Field(
        input_processor=preprocess_text,
        output_processor=lambda x: final_processing(x, extract_state),
    )
    state_abbreviation = Field(
        input_processor=preprocess_text,
        output_processor=lambda x: final_processing(x, extract_state_abbreviation),
    )
    country = Field(
        input_processor=preprocess_text,
        output_processor=lambda x: final_processing(x, extract_country),
    )
    description = Field(
        input_processor=preprocess_text,
        output_processor=lambda x: final_processing(x, extract_description),
    )
    raw_text = Field(input_processor=preprocess_text, output_processor=pick_first)

    # # Housekeeping fields
    # url = Field()
    # project = Field()
    # spider = Field()
    # server = Field()
    # date = Field()
