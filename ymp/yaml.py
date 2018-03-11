import logging
import io
from collections.abc import (
    Mapping, Sequence, MappingView, ItemsView, KeysView, ValuesView
)

from ruamel.yaml import YAML, RoundTripRepresenter

log = logging.getLogger(__name__)


class MixedTypeError(Exception):
    """Mixed types in proxy collection"""


class LayeredConfError(Exception):
    """Error in LayeredConf"""


class LayeredConfWriteError(LayeredConfError):
    """Can't write"""


class LayeredConfAccessError(LayeredConfError, KeyError, IndexError):
    """Can't access"""


class AttrItemAccessMixin(object):
    """Mixin class mapping dot to bracket access

    Added to classes implementing __getitem__, __setitem__ and
    __delitem__, this mixin will allow acessing items using dot
    notation. I.e. "object.xyz" is translated to "object[xyz]".
    """
    def __getattr__(self, key):
        try:
            if key[0] == "_":
                return self.__getattribute__(key)
            else:
                return self[key]
        except (IndexError, KeyError) as e:
            raise AttributeError(e)

    def __setattr__(self, key, value):
        try:
            if key[0] == "_":
                object.__setattr__(self, key, value)
            else:
                self[key] = value
        except (IndexError, KeyError) as e:
            raise AttributeError(e)

    def __delattr__(self, key):
        raise NotImplementedError()


class MultiProxy(object):
    """Base class for layered container structure"""
    def __init__(self, maps, parent=None):
        self._maps = list(maps)
        self._parent = parent

    def make_map_proxy(self, items):
        return self.__class__(items, parent=self)

    def make_seq_proxy(self, items):
        return MultiSeqProxy(items, parent=self)

    def to_yaml(self, show_source=False):
        buf = io.StringIO()
        if show_source:
            for fn, layer in self._maps:
                buf.write(f"--- # from '{fn}' # ---\n")
                rt_yaml.dump(layer, buf)
        else:
            rt_yaml.dump(self, buf)
        return buf.getvalue()

    def __str__(self):
        return self.to_yaml()

    def __repr__(self):
        return f"{self.__class__.__name__}({self._maps!r})"


class MultiMapProxyMappingView(MappingView):
    """MappingView for MultiMapProxy"""
    def __init__(self, mapping):
        self._mapping = mapping

    def __len__(self):
        return len(self._mapping)

    def __repr__(self):
        return '{0.__class__.__name__}({0._mapping!r})'.format(self)


class MultiMapProxyItemsView(MultiMapProxyMappingView, ItemsView):
    """ItemsView for MultiMapProxy"""
    def __contains__(self, key):
        return (isinstance(key, tuple)
                and len(key) == 2
                and key[0] in self._mapping
                and self._mapping[key[0]] == key[1])

    def __iter__(self):
        for key in self._mapping:
            yield key, self._mapping[key]


class MultiMapProxyKeysView(MultiMapProxyMappingView, KeysView):
    """KeysView for MultiMapProxy"""
    def __contains__(self, key):
        return key in self._mapping

    def __iter__(self):
        yield from iter(self._mapping)


class MultiMapProxyValuesView(MultiMapProxyMappingView, ValuesView):
    """ValuesView for MultiMapProxy"""
    def __contains__(self, key):
        return any(self._mapping[k] == key for k in self._mapping)

    def __iter__(self):
        for key in self._mapping:
            yield self._mapping[key]


class MultiMapProxy(Mapping, MultiProxy, AttrItemAccessMixin):
    """Mapping Proxy for layered containers"""
    def __contains__(self, key):
        return any(key in m for _, m in self._maps)

    def __len__(self):
        return len(set(k for _, m in self._maps for k in m))

    def __getitem__(self, key):
        items = [(fn, m[key]) for fn, m in self._maps if key in m]
        if not items:
            raise KeyError(f"key '{key}' not found in any map")
        typs = set(type(m[1]) for m in items if m)
        if len(typs) > 1:
            raise MixedTypeError(
                f"while trying to obtain '{key}' from {items!r},"
                f"types differ: {typs}"
            )
        if isinstance(items[0][1], Mapping):
            return self.make_map_proxy(items)
        if isinstance(items[0][1], str):
            return items[0][1]
        if isinstance(items[0][1], Sequence):
            return self.make_seq_proxy(items)
        return items[0][1]

    def __iter__(self):
        for key in set(k for _, m in self._maps for k in m):
            yield key

    def get(self, value, default=None):
        try:
            return self[value]
        except KeyError:
            return default

    def items(self):
        return MultiMapProxyItemsView(self)

    def keys(self):
        return MultiMapProxyKeysView(self)

    def values(self):
        return MultiMapProxyValuesView(self)

    def __setitem__(self, key, value):
        self._maps[0][1][key] = value

    def __delitem__(self, key):
        raise NotImplementedError()

    def __missing__(self, key):
        raise NotImplementedError()


class MultiSeqProxy(Sequence, MultiProxy, AttrItemAccessMixin):
    """Sequence Proxy for layered containers"""
    def __contains__(self, value):
        return any(value in m for _, m in self._maps)

    def __len__(self):
        return sum(len(m) for _, m in self._maps)

    def __repr__(self):
        return f"{self.__class__.__name__}({self._maps})"

    def __str__(self):
        return "+".join(f"{m}" for _, m in self._maps)

    def __getitem__(self, index):
        if isinstance(index, slice):
            raise NotImplementedError()
        if isinstance(index, str):
            try:
                index = int(index)
            except ValueError:
                raise KeyError()
        for _, m in self._maps:
            if index >= len(m):
                index -= len(m)
            else:
                return m[index]

    def __setitem__(self, key, value):
        raise NotImplementedError()

    def __delitem__(self, key):
        raise NotImplementedError()

    def __missing__(self, key):
        raise NotImplementedError()

    def __iter__(self):
        for _, m in self._maps:
            for item in m:
                yield item


class LayeredConfProxy(MultiMapProxy):
    """Layered configuration"""


RoundTripRepresenter.add_representer(LayeredConfProxy,
                                     RoundTripRepresenter.represent_dict)
RoundTripRepresenter.add_representer(MultiMapProxy,
                                     RoundTripRepresenter.represent_dict)
RoundTripRepresenter.add_representer(MultiSeqProxy,
                                     RoundTripRepresenter.represent_list)

rt_yaml = YAML(typ="rt")


def load(files):
    """Load configuration files

    Creates a `LayeredConfProxy` configuration object from a set of
    YAML files.
    """
    layers = []
    for fn in reversed(files):
        with open(fn, "r") as f:
            yaml = rt_yaml.load(f)
            if not isinstance(yaml, dict):
                raise LayeredConfError(
                    f"Malformed config file '{fn}'."
                )
            layers.append((fn, yaml))
    return LayeredConfProxy(layers)
