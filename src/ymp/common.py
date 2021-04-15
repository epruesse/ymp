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
        except KeyError as e:
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


class NoCache(object):
    def __init__(self, root):
        self.caches = {}

    def close(self):
        pass  # NoCache doesn't close anything

    def get_cache(self, name, clean=False, *args, **kwargs):
        if name not in self.caches:
            self.caches[name] = CacheDict(self, name, *args, **kwargs)
        return self.caches[name]

    def store(self, cache, key, obj):
        pass  # NoCache doesnt store anything

    def commit(self):
        pass # NoCache doesnt commit anything

    def load(self, _cache, _key):
        return None

    def load_all(self, _cache):
        return ()


class Cache(object):
    def __init__(self, root):
        import sqlite3
        os.makedirs(os.path.join(root), exist_ok=True)
        db_fname = os.path.join(root, "ymp.db")
        log.debug("Opening database %s", db_fname)
        self.conn = sqlite3.connect(db_fname, check_same_thread=False)

        # Drop tables if the database has the wrong version number
        # or if the user_version has not been set (defaults to 0)
        version = self.conn.execute("PRAGMA user_version").fetchone()[0]
        if version == ymp.__numeric_version__ and version != 0:
            try:
                curs = self.conn.execute("SELECT file, time from stamps")
                update = any(os.path.getmtime(row[0]) > row[1] for row in curs)
            except FileNotFoundError:
                update = True
            del curs
            if update:
                log.error("Dropping cache: files changed")
                self.conn.executescript("""
                DROP TABLE caches;
                DROP TABLE stamps;
                """)
        else:
            log.info("No cache, loading...")
            update = True

        if update:
            self.conn.executescript("""
            BEGIN EXCLUSIVE;
            DROP TABLE IF EXISTS caches;
            CREATE TABLE caches (
                name TEXT,
                key TEXT,
                data,
                PRIMARY KEY (name, key)
            );
            DROP TABLE IF EXISTS stamps;
            CREATE TABLE stamps (
                file TEXT PRIMARY KEY,
                time INT
            );

            PRAGMA user_version={};
            COMMIT;
            """.format(ymp.__numeric_version__))

        self.caches = {}
        self.files = {}

    def close(self):
        self.conn.close()

    def get_cache(self, name, clean=False, *args, **kwargs):
        if name not in self.caches:
            self.caches[name] = CacheDict(self, name, *args, **kwargs)
        return self.caches[name]

    def store(self, cache, key, obj):
        import pickle

        files = ensure_list(getattr(obj, "defined_in", None))
        try:
            stamps = [(fn, os.path.getmtime(fn))
                      for fn in files
                      if fn not in self.files]
            self.conn.executemany(
                "REPLACE INTO stamps VALUES (?,?)",
                stamps)
            self.files.update(dict(stamps))
            self.conn.execute("""
              REPLACE INTO caches
              VALUES (?, ?, ?)
            """, [cache, key, pickle.dumps(obj)]
            )
        except FileNotFoundError:
            pass

    def commit(self):
        import sqlite3
        try:
            self.conn.commit()
        except sqlite3.OperationalError as e:
            log.warning("Cache write failed: %s", e.what())

    def load(self, cache, key):
        import pickle
        row = self.conn.execute("""
        SELECT data FROM caches WHERE name=? AND key=?
        """, [cache, key]).fetchone()
        if row:
            obj = pickle.loads(row[0])
            try:
                obj.load_from_pickle()
            except AttributeError:
                pass
            return obj
        else:
            return None

    def load_all(self, cache):
        import pickle
        rows = self.conn.execute("""
        SELECT key, data FROM caches WHERE name=?
        """, [cache])
        return ((row[0], pickle.loads(row[1]))
                for row in rows)


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
        self._complete = False

    def _loaditem(self, key):
        cached = self._cache.load(self._name, key)
        if cached:
            super().__setitem__(key, cached)
        elif self._itemdata is not None:
            if key in self._itemdata:
                item = self._itemloadfunc(key, self._itemdata[key])
                self._cache.store(self._name, key, item)
                self._cache.commit()
                super().__setitem__(key, item)
        elif self._itemloadfunc:
            item = self._itemloadfunc(key)
            self._cache.store(self._name, key, item)
            self._cache.commit()
            super().__setitem__(key, item)
        else:
            self._loadall()

    def _loadall(self):
        if self._complete:
            return
        loaded = set()
        for key, obj in self._cache.load_all(self._name):
            loaded.add(key)
            super().__setitem__(key, obj)
        if self._itemloadfunc:
            for key in self._itemdata:
                if key not in loaded:
                    self._loaditem(key)
        elif self._loadfunc and not self._loading and not loaded:
            self._loadfunc(*self._args, **self._kwargs)
            self._loadfunc = None
            for key, item in super().items():
                self._cache.store(self._name, key, item)
            self._cache.commit()
        self._complete = True

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
        if not super().__contains__(key):
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

    def __str__(self):
        self._loadall()
        return super().__str__()

    def get(self, key, default=None):
        if not super().__contains__(key):
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
