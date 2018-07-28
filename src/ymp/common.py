"""
Collection of shared utility classes and methods
"""
import logging
import os
from collections import Iterable, Mapping, OrderedDict

log = logging.getLogger(__name__)


class OrderedDictMaker(object):
    """
    odict creates OrderedDict objects in a dict-literal like syntax

    >>>  my_ordered_dict = odict[
    >>>    'key': 'value'
    >>>  ]

    Implementation:
    odict uses the python slice syntax which is similar to dict literals.
    The [] operator is implemented by overriding __getitem__. Slices
    passed to the operator as ``object[start1:stop1:step1, start2:...]``,
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
    """Recursively update dictionary ``dst`` with ``src``

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
            try:
                val = self[attr]
            except KeyError as e:
                raise AttributeError(e)
            if isinstance(val, dict):
                return AttrDict(val)
            else:
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
    """Wrap ``obj`` in a `list` as needed"""
    if obj is None:
        return []
    if isinstance(obj, str) or not isinstance(obj, Iterable):
        return [obj]
    return list(obj)


class Cache(object):
    def __init__(self, root):
        import sqlite3
        os.makedirs(os.path.join(root, ".ymp"), exist_ok=True)
        self.conn = sqlite3.connect(os.path.join(root, ".ymp", "ymp.db"))
        try:
            self.conn.executescript("""
            CREATE TABLE caches (
              name TEXT,
              files TEXT,
              data
            );
            CREATE TABLE stamps (
              file TEXT,
              time NUM
            );
            """)
        except sqlite3.OperationalError:
            pass
        self.caches = {}

    def close(self):
        self.conn.close()

    def get_cache(self, name, clean=False, *args, **kwargs):
        if name not in self.caches:
            self.caches[name] = CacheDict(self, name, *args, **kwargs)
        return self.caches[name]


class CacheDict(AttrDict):
    def __init__(self, cache, name, *args, loadfunc=None,
                 itemloadfunc=None, itemdata=None, **kwargs):
        self._cache = cache
        self._name = name
        self._loadfunc = loadfunc
        self._itemloadfunc = itemloadfunc
        self._itemdata = itemdata
        self._args = args
        self._kwargs = kwargs
        self._loading = False

    def _loaditem(self, key):
        if self._itemdata is not None:
            if key in self._itemdata:
                item = self._itemloadfunc(key, self._itemdata[key])
                super().__setitem__(key, item)
        elif self._itemloadfunc:
            item = self._itemloadfunc(key)
            super().__setitem__(key, item)
        else:
            self._loadall()

    def _loadall(self):
        if self._itemloadfunc:
            for key in self._itemdata:
                self._loaditem(key)
        elif self._loadfunc and not self._loading:
            self._loadfunc(*self._args, **self._kwargs)
            self._loadfunc = None

    def __enter__(self):
        self._loading = True
        return self

    def __exit__(self, a, b, c):
        self._loading = False

    def __contains__(self, key):
        if self._itemdata:
            return key in self._itemdata
        self._loadall()
        return super().__contains__(key)

    def __len__(self):
        if self._itemdata:
            return len(self._itemdata)
        self._loadall()
        return super().__len__()

    def __getitem__(self, key):
        self._loaditem(key)
        return super().__getitem__(key)

    def __setitem__(self, key, val):
        super().__setitem__(key, val)

    def __delitem__(self, key):
        raise NotImplementedError()
        super().__delitem__(key)

    def __iter__(self):
        if self._itemdata:
            return self._itemdata.__iter__()
        self._loadall()
        return super().__iter__()

    def get(self, key, default=None):
        self._loaditem(key)
        return super().get(key, default)

    def items(self):
        self._loadall()
        return super().items()

    def keys(self):
        if self._itemdata:
            return self._itemdata.keys()
        return super().keys()

    def values(self):
        self._loadall()
        return super().values()
