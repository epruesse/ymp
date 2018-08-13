import io
import logging
import os
from collections.abc import (
    ItemsView, KeysView, Mapping, MappingView, Sequence, ValuesView
)

import ruamel.yaml
from ruamel.yaml import RoundTripRepresenter, YAML
from ruamel.yaml.comments import CommentedMap


log = logging.getLogger(__name__)  # pylint: disable=invalid-name


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
    def __init__(self, maps, parent=None, key=None):
        self._maps = list(maps)
        self._parent = parent
        self._key = key

    def make_map_proxy(self, key, items):
        return MultiMapProxy(items, parent=self, key=key)

    def make_seq_proxy(self, key, items):
        return MultiSeqProxy(items, parent=self, key=key)

    def get_files(self):
        return [fn for fn, layer in self._maps]

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

    def add_layer(self, name, container):
        self._maps.insert(0, ((name, container)))

    def remove_layer(self, name):
        map_name = self._maps[0][0]
        if map_name == name:
            self._maps.pop(0)
        else:
            raise LayeredConfError(f"in remove_layer: {map_name} != {name}")


class MultiMapProxyMappingView(MappingView):
    """MappingView for MultiMapProxy"""
    def __init__(self, mapping):
        self._mapping = mapping

    def __len__(self):
        return len(self._mapping)

    def __repr__(self):
        return '{0.__class__.__name__}({0._mapping!r})'.format(self)

    def __radd__(self, other):
        if isinstance(other, Sequence):
            return other + list(self)
        raise TypeError()


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
        typs = set(type(m[1]) for m in items if m[1])
        if len(typs) > 1:
            raise MixedTypeError(
                f"while trying to obtain '{key}' from {items!r},"
                f"types differ: {typs}"
            )
        if isinstance(items[0][1], Mapping):
            return self.make_map_proxy(key, items)
        if isinstance(items[0][1], str):
            return items[0][1]
        if isinstance(items[0][1], Sequence):
            return self.make_seq_proxy(key, items)
        return items[0][1]

    def __setitem__(self, key, value):
        # we want to set to the top layer, so get that first
        mp = self
        keys = []
        while mp._parent:
            keys.append(mp._key)
            mp = mp._parent
        # now walk back down
        for k in reversed(keys):
            if k not in mp._maps[0][1]:
                mp._maps[0][1][k] = CommentedMap()
            mp = mp[k]
        # and set the value, potentially on a different object than self
        mp._maps[0][1][key] = value

    def __delitem__(self, key):
        raise NotImplementedError()

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


class MultiSeqProxy(Sequence, MultiProxy, AttrItemAccessMixin):
    """Sequence Proxy for layered containers"""
    def __contains__(self, value):
        return any(value in m for _, m in self._maps)

    def __iter__(self):
        for _, m in self._maps:
            for item in m:
                yield item

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

    def __radd__(self, other):
        return self.__add__(other)

    def __add__(self, other):
        return other + list(self)

    def __setitem__(self, key, value):
        raise NotImplementedError()

    def __delitem__(self, key):
        raise NotImplementedError()

    def __missing__(self, key):
        raise NotImplementedError()

    def __iadd__(self, item):  # broken?!
        self.extend(item)

    def extend(self, item):
        m = self._maps[0][1]
        m.extend(item)


class LayeredConfProxy(MultiMapProxy):
    """Layered configuration"""

    def __str__(self):
        try:
            return self.to_yaml()
        except ruamel.yaml.serializer.SerializerError:
            return self.__repr__()

    def __enter__(self):
        self.add_layer("dynamic", {})
        return self

    def __exit__(self, *args):
        self.remove_layer("dynamic")

    def save(self, outstream=None, layer=0):
        outfile = None
        if outstream:
            rt_yaml.dump(self._maps[layer][1], outstream)
        else:
            outfile = self._maps[layer][0]
            with open(outfile+".tmp", "w") as outstream:
                rt_yaml.dump(self._maps[layer][1], outstream)
            os.rename(outfile+".tmp", outfile)


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
            if not isinstance(yaml, Mapping):
                raise LayeredConfError(
                    f"Malformed config file '{fn}'."
                )
            layers.append((fn, yaml))
    return LayeredConfProxy(layers)
