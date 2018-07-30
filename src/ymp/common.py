"""
Collection of shared utility classes and methods
"""
import logging
import os
from collections import Iterable

log = logging.getLogger(__name__)



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
        self.conn = sqlite3.connect(os.path.join(root, ".ymp", "ymp.db"), check_same_thread = False)
        # TODO:
        # - maintain a cache version
        # - check file stamps
        # - use XDG cache directory if we are outside a working directory

        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS caches (
            name TEXT,
            key TEXT,
            data,
            PRIMARY KEY (name, key)
        );
        CREATE TABLE IF NOT EXISTS stamps (
            file TEXT PRIMARY KEY,
            time NUM
        );
        """)
        self.conn.commit()
        self.caches = {}

    def close(self):
        for cache in self.caches.values():
            cache.close()
        self.conn.close()

    def get_cache(self, name, clean=False, *args, **kwargs):
        if name not in self.caches:
            self.caches[name] = CacheDict(self, name, *args, **kwargs)
        return self.caches[name]

    def store(self, cache, key, data):
        self.conn.execute("""
          REPLACE INTO caches
          VALUES (?, ?, ?)
        """, [cache, key, data]
        )

    def load(self, cache, key):
        row = self.conn.execute("""
        SELECT data FROM caches WHERE name=? AND key=?
        """, [cache, key]).fetchone()
        if row:
            return row[0]
        else:
            return None

    def load_all(self, cache):
        rows = self.conn.execute("""
        SELECT key, data FROM caches WHERE name=?
        """, [cache])
        return rows


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

    def close(self):
        pass

    def _loaditem(self, key):
        import pickle
        cached = self._cache.load(self._name, key)
        if cached:
            super().__setitem__(key, pickle.loads(cached))
        elif self._itemdata is not None:
            if key in self._itemdata:
                item = self._itemloadfunc(key, self._itemdata[key])
                self._cache.store(self._name, key, pickle.dumps(item))
                self._cache.conn.commit()
                super().__setitem__(key, item)
        elif self._itemloadfunc:
            item = self._itemloadfunc(key)
            self._cache.store(self._name, key, pickle.dumps(item))
            self._cache.conn.commit()
            super().__setitem__(key, item)
        else:
            self._loadall()

    def _loadall(self):
        import pickle
        loaded = set()
        cached = self._cache.load_all(self._name)
        for row in cached:
            loaded.add(row[0])
            super().__setitem__(row[0], pickle.loads(row[1]))
        if self._itemloadfunc:
            for key in self._itemdata:
                if key not in loaded:
                    self._loaditem(key)
        elif self._loadfunc and not self._loading and not loaded:
            self._loadfunc(*self._args, **self._kwargs)
            self._loadfunc = None
            for key, item in super().items():
                self._cache.store(self._name, key, pickle.dumps(item))
            self._cache.conn.commit()

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
