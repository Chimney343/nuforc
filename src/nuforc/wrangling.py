import hashlib
import logging
import re
from datetime import datetime

import parsedatetime
import us
from iso3166 import countries

from nuforc.lookups.geography_lookups import (
    CAN_PROVINCE_NAMES,
    NON_ISO_3166_COUNTRY_NAMES,
)

from nuforc.regexes import REGEX_DICT
logger = logging.getLogger("model.modules.wrangling")

"""
Raw NUFORC event text wrangling functions.
"""

def preprocess_text(response):
    text = " ".join(response).strip()
    return text


def extract_description(text):
    try:
        info = "".join(text.splitlines()[2:]).strip()
        return info
    except:
        return "unparsed"


def extract_url(text):
    try:
        info = text.splitlines()[0].strip()
        return info
    except:
        return "unparsed"


def extract_city(text):
    location_match = REGEX_DICT["location"].search(text)
    if location_match is not None:
        location_match = location_match.group()
        try:
            info = get_city_from_location(location_match)
            return info
        except:
            return "unparsed"


def extract_state_abbreviation(text):
    location_match = REGEX_DICT["location"].search(text)
    if location_match is not None:
        location_match = location_match.group()
        try:
            info = get_state_info(location_match)["state_abbreviation"]
            return info
        except:
            return "unparsed"


def extract_state(text):
    location_match = REGEX_DICT["location"].search(text)
    if location_match is not None:
        location_match = location_match.group()
        try:
            info = get_state_info(location_match)["state"].name
            return info
        except:
            return "unparsed"


def extract_country(text):
    location_match = REGEX_DICT["location"].search(text)
    if location_match is not None:
        location_match = location_match.group()
        try:
            info = get_country_from_location(location_match)
            return info
        except:
            return "unparsed"


def extract_duration(text):
    if isinstance(text, list):
        text = text[0]

    match = REGEX_DICT["duration"].search(text)
    if match is not None:
        time_string = match.group()
        return parse_duration(time_string)
    else:
        return "unparsed"


def extract_time(text, time_type):
    match = REGEX_DICT[time_type].findall(text)
    if match is not None:
        try:
            match = [s for s in match[0] if s != ""][1:]
            event_date = match[0]
            event_time = ":".join(match[1:])
            event_datetime = " ".join([event_date, event_time])
            return event_datetime
        except:
            return "unparsed"


def extract_shape(text):
    match = REGEX_DICT["shape"].search(text)
    if match is not None:
        info = match.group()
        return info.lower()
    else:
        return "unparsed"


def get_state_info(location):
    us_state_abbreviation_regex = re.compile(
        "(A[KLRZ]|C[AOT]|D[CE]|FL|GA|HI|I[ADLN]|K[SY]|LA|M[ADEINOST]|N[CDEHJMVY]|O[HKR]|PA|RI|S[CD]|T[NX]|UT|V[AT]|W[AIVY])",
        re.VERBOSE,
    )

    canadian_state_abbreviation_regex = re.compile(
        r"(N[BLSTU]|[AMN]B|[BQ]C|ON|PE|SK)",
        re.VERBOSE,
    )

    if us_state_abbreviation_regex.search(location) is not None:
        us_state_abbreviation = us_state_abbreviation_regex.search(location).group()
        us_state = us.states.lookup(us_state_abbreviation)
        if us_state is not None:
            state_info = {
                "state": us_state,
                "state_abbreviation": us_state_abbreviation,
                "country": "USA",
            }

            return state_info

    elif canadian_state_abbreviation_regex.search(location) is not None:
        canadian_state_abbreviation = canadian_state_abbreviation_regex.search(
            location
        ).group()
        canadian_state = CAN_PROVINCE_NAMES.get(canadian_state_abbreviation)
        if canadian_state is not None:
            state_info = {
                "state": canadian_state,
                "state_abbreviation": canadian_state_abbreviation,
                "country": "Canada",
            }
            return state_info


def get_valid_country_name(name, custom_lookup=NON_ISO_3166_COUNTRY_NAMES):
    """

    Args:
        name:
        custom_lookup:

    Returns:

    """

    # Characters to remove.
    characters_to_remove = "+:,"
    pattern = "[" + characters_to_remove + "]"
    name = re.sub(pattern, "", name)
    # Wrangle the string.
    name = name.strip()
    name = name.lower()

    if custom_lookup.get(name) is not None:
        return custom_lookup.get(name)
    elif countries.get(name, None) is not None:
        valid_name = countries.get(name)[0]
        return valid_name


def get_country_from_location(location):
    # If location contains state from either USA or Canada, state_info will be not None and will contain country name.
    state_info = get_state_info(location)
    if state_info is not None:
        return state_info["country"]

    # If location does not contain state name, country information is stored inside brackets and has to checked if
    # it's a valid country name.
    if get_valid_country_name(location) is not None:
        return get_valid_country_name(location)

    # If location string does not contain a state name nor is a valid country name itself, country information is
    # stored inside brackets and has to extracted with a regex.
    regex = re.compile(r"\((.*?)\)")
    match = regex.findall(location)
    if match is not None and match != []:
        match = match[-1] if len(match) > 1 else match[0]
    if "/" in match:
        return get_valid_country_name(match.split("/")[0])
    elif "," in match:
        return get_valid_country_name(match.split(",")[0])
    else:
        return get_valid_country_name(match)


def get_city_from_location(location):
    # If location contains brackets, take the sequence before the first bracket.
    regex = re.compile(r".+?(?=\()")
    match = regex.search(location)
    if match is not None:
        match = match.group()
        if "/" in match:
            return match.split("/")[0].strip()
        elif "," in match:
            return match.split(",")[0].strip()
        else:
            return match.strip()

    #  If name doesn't contain brackets, take it as is and check if it's a country.
    if "/" in location:
        return location.split("/")[0].strip()
    elif "," in location:
        return location.split(",")[0].strip()
    elif get_valid_country_name(location) is not None:
        return None
    else:
        return location.strip()

#TODO: Consider moving this to geocoder lib.
def create_address(city, state, country):
    values = [str(val) for val in (city, state, country) if val is not None]
    return ', '.join(values)

def parse_duration(t):
    t = clean_time_string(t)

    basetime = datetime.now().replace(microsecond=0)
    cal = parsedatetime.Calendar()
    time_struct, parse_status = cal.parse(t, sourceTime=basetime)
    if parse_status == 0:
        return None

    parsed = datetime(*time_struct[:6])
    time = parsed - basetime
    return time


def parse_time(t):
    cal = parsedatetime.Calendar()
    time_struct, parse_status = cal.parse(t)
    if parse_status == 0:
        return None
    else:
        time = datetime(*time_struct[:6])
        return time


def clean_time_string(t):
    t = str(t).lower().strip()
    # Splits.
    if "-" in t:
        t = t.split("-")[1]
    # Characters to remove.
    characters_to_remove = "+:"
    pattern = "[" + characters_to_remove + "]"
    t = re.sub(pattern, "", t)

    # Sequences to replace.
    replacements = {"hrs": "hours", ":": "", "mintues": "minutes"}

    replacements = dict((re.escape(k), v) for k, v in replacements.items())
    pattern = re.compile("|".join(replacements.keys()))
    t = pattern.sub(lambda m: replacements[re.escape(m.group(0))], t)
    return t

def hash_string(s):
    hash_object = hashlib.sha256()
    hash_object.update(s.encode())
    return hash_object.hexdigest()