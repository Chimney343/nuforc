import re

REGEX_DICT = {
    "occurred_time": re.compile(
        r"""
(Occurred\s:\s
|
Occurred:\s)
([0-3]?[0-9]/[0-3]?[0-9]/(?:[0-9]{2})?[0-9]{2})\s?(2[0-3]|[01]?
[0-9]):([0-5]?[0-9]):?([0-5]?[0-9])?
""",
        re.VERBOSE,
    ),
    "entered_as_time": re.compile(
        r"""
(Entered\sas\s:\s
|
Entered\sas:\s)
([0-3]?[0-9]/[0-3]?[0-9]/(?:[0-9]{2})?[0-9]{2})\s?(2[0-3]|[01]?
[0-9]):([0-5]?[0-9]):?([0-5]?[0-9])?
""",
        re.VERBOSE,
    ),
    "reported_time": re.compile(
        r"""
(Reported:\s
|
Reported\s:\s)
([0-3]?[0-9]/[0-3]?[0-9]/(?:[0-9]{2})?[0-9]{2})\s?(2[0-3]|[01]?
[0-9]):([0-5]?[0-9]):?([0-5]?[0-9])?
""",
        re.VERBOSE,
    ),
    "location": re.compile(
        r"""
(?<=Location:\s)(.*)(?=Shape)
""",
        re.VERBOSE,
    ),
    "shape": re.compile(
        r"""
(?<=Shape:\s)(.*)(?=Duration)
""",
        re.VERBOSE,
    ),
    "duration": re.compile(
        r"""
(?<=Duration:\s)|(?<=Duration:)(.*)
""",
        re.VERBOSE,
    ),
    "description": None,
    "url": None,
}