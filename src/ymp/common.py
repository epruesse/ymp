"""
Collection of shared utility classes and methods
"""
import logging
import re
import os
from collections.abc import Iterable

import ymp

log = logging.getLogger(__name__)


class AttrDict(dict):
    """
    AttrDict adds accessing stored keys as attributes to dict
    """
    def __getattr__(self, attr):
        try:
            val = self[attr]
        except KeyError:
            raise AttributeError(f'{self} has no attribute {attr}') from None
        if isinstance(val, dict):
            return AttrDict(val)
        return val

    def __setattr__(self, attr, value):
        if attr.startswith("_"):
            super().__setattr__(attr, value)
        else:
            raise NotImplementedError()


class MkdirDict(AttrDict):
    "Creates directories as they are requested"
    def __getattr__(self, attr):
        dirname = super().__getattr__(attr)
        if not os.path.exists(dirname):
            log.info("Creating directory %s", dirname)
            os.makedirs(dirname, exist_ok=True)
        return dirname


def parse_number(s=""):
    """Basic 1k 1m 1g 1t parsing.

    - assumes base 2
    - returns "byte" value
    - accepts "1kib", "1kb" or "1k"
    """
    multiplier = 1
    s = str(s).strip().upper().rstrip("BI")

    if s.endswith("K"):
        multiplier = 1024
    if s.endswith("M"):
        multiplier = 1024*1024
    if s.endswith("G"):
        multiplier = 1024*1024*1024
    if s.endswith("T"):
        multiplier = 1024*1024*1024*1024

    s = s.rstrip("KMGT")

    if "." in s:
        return float(s)*multiplier
    else:
        return int(s)*multiplier


def format_number(num: int, unit="") -> int:
    div = parse_number("1"+unit) if unit else 1
    return round(num / div)


TIME_RE = re.compile(
    r"""
    (?P<days>\d+(?=-))?-?
    (?P<hours>(?<=-)\d{1,2}|\d{1,2}(?=:\d{1,2}:)|):?
    (?P<minutes>\d{1,2})?:?
    (?P<seconds>\d{1,2})?
    """,
    re.VERBOSE
)

def parse_time(timestr: str) -> int:
    """Parses time in "SLURM" format

    <minutes>
    <minutes>:<seconds>
    <hours>:<minutes>:<seconds>
    <days>-<hours>
    <days>-<hours>:<minutes>
    <days>-<hours>:<minutes>:<seconds>
    """
    match = TIME_RE.match(str(timestr))
    if not match:
        raise ValueError()
    data = match.groupdict()
    return (
        int(data.get("days") or 0) * 86400
        + int(data.get("hours") or 0) * 3600
        + int(data.get("minutes") or 0) * 60
        + int(data.get("seconds") or 0)
    )

def format_time(seconds: int, unit=None) -> str:
    """Prints time in SLURM format"""
    if unit in ("s", "seconds"):
        return str(seconds)
    if unit in ("m", "minutes"):
        return str(round(seconds/60))
    days = seconds // 86400
    seconds = seconds % 86400
    hours = seconds // 3600
    seconds = seconds % 3600
    minutes = seconds // 60
    seconds = seconds % 60
    return f"{days}-{hours}:{minutes}:{seconds}"


def flatten(item):
    """Flatten lists without turning strings into letters"""
    if isinstance(item, str):
        yield item
    elif isinstance(item, Iterable):
        for item2 in item:
            yield from flatten(item2)
    else:
        yield item


def is_container(obj):
    """Check if object is container, considering strings not containers"""
    return not isinstance(obj, str) and isinstance(obj, Iterable)


def ensure_list(obj):
    """Wrap ``obj`` in a `list` as needed"""
    if obj is None:
        return []
    if isinstance(obj, str) or not isinstance(obj, Iterable):
        return [obj]
    return list(obj)


