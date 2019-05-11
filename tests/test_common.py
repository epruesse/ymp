import ymp
from ymp.common import Cache


class LoadFuncs(object):
    def __init__(self):
        self.count = 0

    def reset(self):
        self.count = 0

    def make_itemfunc(self):
        def itemloadfunc(key, arg=None):
            self.count += 1
            return key
        return itemloadfunc


def test_cache_version(saved_cwd):
    fact = LoadFuncs()
    cache = Cache(saved_cwd)
    test_cache = cache.get_cache("test", itemloadfunc=fact.make_itemfunc())
    assert fact.count == 0
    assert test_cache[1] == 1
    # first run of new cache, itemfunc should be called once:
    assert fact.count == 1
    cache.close()

    fact.reset()
    cache = Cache(saved_cwd)
    test_cache = cache.get_cache("test", itemloadfunc=fact.make_itemfunc())
    assert fact.count == 0
    assert test_cache[1] == 1
    # second run of new cache, itemfunc should not have been called
    assert fact.count == 0
    cache.close()

    # alter version
    true_version = ymp.__numeric_version__
    ymp.__numeric_version__ = -1

    fact.reset()
    cache = Cache(saved_cwd)
    test_cache = cache.get_cache("test", itemloadfunc=fact.make_itemfunc())
    assert fact.count == 0
    assert test_cache[1] == 1
    # run with changed version, expecting one call
    assert fact.count == 1
    cache.close()

    # restore version
    ymp.__numeric_version__ == true_version
