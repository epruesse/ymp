"""
Collection of shared utility classes and methods
"""
import atexit
import asyncio
import logging
import shelve
import os
from collections import Iterable, Mapping, OrderedDict
from typing import List
from urllib.parse import urlsplit

import aiohttp
from tqdm import tqdm
import xdg

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class OrderedDictMaker(object):
    """
    odict creates OrderedDict objects in a dict-literal like syntax

    Usage:
    my_ordered_dict = odict[
    'key': 'value'
    ]

    Implementation:
    odict uses the python slice syntax which is similar to dict literals.
    The [] operator is implemented by overriding __getitem__. Slices
    passed to the operator as `object[start1:stop1:step1, start2:...]`,
    are passed to the implementation as a list of objects with start, stop
    and step members. odict simply creates an OrderedDictionary by iterating
    over that list.
    """

    def __getitem__(self, keys):
        if isinstance(keys, slice):
            return OrderedDict([(keys.start, keys.stop)])
        return OrderedDict([(slice.start, slice.stop) for slice in keys])


odict = OrderedDictMaker()  # pylint: disable=invalid-name


def update_dict(dst, src):
    """Recursively update dictionary `dst` with `src`

    - Treats a `list` as atomic, replacing it with new list.
    - Dictionaries are overwritten by item
    - None is replaced by empty dict if necessary
    """
    if src is None:
        return dst
    for key, val in src.items():
        if isinstance(val, Mapping):
            dst_sub = dst.get(key, {})
            if (dst_sub) is None:
                dst_sub = {}
            tmp = update_dict(dst_sub, val)
            dst[key] = tmp
        else:
            dst[key] = src[key]
    return dst


class AttrDict(dict):
    """
    AttrDict adds accessing stored keys as attributes to dict
    """
    def __getattr__(self, attr):
        try:
            return super().__getattr__(attr)
        except AttributeError:
            val = self[attr]
            if isinstance(val, dict):
                return AttrDict(val)
            else:
                return val


class MkdirDict(AttrDict):
    "Creates directories as they are requested"
    def __getattr__(self, attr):
        dirname = super().__getattr__(attr)
        if not os.path.exists(dirname):
            log.info("Creating directory %s", dirname)
            os.makedirs(dirname)
        return dirname


def parse_number(s=""):
    """Basic 1k 1m 1g 1t parsing.

    - assumes base 2
    - returns "byte" value
    - accepts "1kib", "1kb" or "1k"
    """
    multiplier = 1
    s = s.strip().upper().rstrip("BI")

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
    """Wrap ``obj`` in a `list()` as needed"""
    if obj is None:
        return []
    if isinstance(obj, str) or not isinstance(obj, Iterable):
        return [obj]
    return list(obj)


class Cache(shelve.DbfilenameShelf):
    _caches = {}

    @classmethod
    def get_cache(cls, name="ymp"):
        if name not in cls._caches:
            return cls(name)
        else:
            return cls._caches[name]

    def __init__(self, name):
        self._cache_basename = os.path.join(
            xdg.XDG_CACHE_HOME,
            'ymp',
            '{}_cache'.format(name)
        )
        os.makedirs(os.path.dirname(self._cache_basename), exist_ok=True)
        atexit.register(Cache.close, self)
        super().__init__(self._cache_basename)
        self._caches[name] = self


class FileDownloader(object):
    """Manages download of a set of URLs

    Downloads happen concurrently using asyncronous network IO.
    """
    def __init__(self, block_size=4096, timeout=60, parallel=4, progress=True):
        self._block_size = block_size
        self._timeout = timeout
        self._parallel = parallel
        self._sem = asyncio.Semaphore(parallel)
        self._progress = progress
        self._sum_bar = None

    @staticmethod
    def make_bar_format(desc_width=20, count_width=0, rate=False, eta=False):
        """Construct bar_format for tqdm

        Args:
          desc_width: minimum space allocated for description
          count_width: min space for counts
          rate: show rate to right of progress bar
          eta: show eta to right of progress bar
        """
        left = '{{desc:<{dw}}} {{percentage:3.0f}}%'.format(dw=desc_width)
        right = ' {{n_fmt:>{cw}}} / {{total_fmt:<{cw}}}'.format(cw=count_width)
        if rate:
            right += ' {{rate_fmt:>{cw}}}'.format(cw=count_width+2)
        if eta:
            right += ' ETA {remaining}'
        return left + '|{bar}|' + right

    async def _get_file(self, session, url, dest):
        parts = urlsplit(url)
        if os.path.isdir(dest):
            dest = os.path.join(dest, os.path.basename(parts.path))

        part = dest+".part"

        try:
            async with self._sem, \
                       session.get(url, timeout=self._timeout) as resp:
                if not resp.status == 200:
                    return
                size = int(resp.headers.get('content-length', 0))
                self._sum_bar.total += size
                with open(part, mode="wb") as out, \
                     tqdm(total=size,
                          unit='B', unit_scale=True, unit_divisor=1024,
                          desc=os.path.basename(dest), leave=False,
                          miniters=1, disable=not self._progress,
                          bar_format=self.make_bar_format(40, 7, rate=True)) as t:
                    while True:
                        block = await resp.content.read(self._block_size)
                        if not block:
                            break
                        out.write(block)
                        t.update(len(block))
                        self._sum_bar.update(len(block))
        except asyncio.CancelledError:
            if os.path.exists(part):
                os.unlink(part)
            raise
        os.rename(part, dest)

    async def _get_all(self, urls, dest):
        with aiohttp.ClientSession() as session:
            coros = [asyncio.ensure_future(self._get_file(session, url, dest))
                     for url in urls]
            with tqdm(asyncio.as_completed(coros), total=len(coros),
                      unit="Files", desc="Total files:",
                      disable=not self._progress,
                      bar_format=self.make_bar_format(20, 7, eta=True)) as t, \
                 tqdm(total=0,
                      unit="B", desc="Total bytes:",
                      unit_scale=True, unit_divisor=1024,
                      disable=not self._progress, miniters=1,
                      bar_format=self.make_bar_format(20, 7, rate=True)) as t2:
                self._sum_bar = t2
                for coro in t:
                    await coro

    def get(self, urls: List[str], dest: str) -> None:
        """Download a list of URLs

        Args:
          urls: List of URLs
          dest: Destination folder
        """
        loop = asyncio.get_event_loop()

        try:
            task = asyncio.ensure_future(self._get_all(urls, dest))
            loop.run_until_complete(task)
        except KeyboardInterrupt:
            end = asyncio.gather(*asyncio.Task.all_tasks())
            end.cancel()
            try:
                loop.run_until_complete(end)
            except asyncio.CancelledError:
                pass
            raise
        finally:
            loop.close()
