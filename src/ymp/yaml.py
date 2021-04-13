import io
import logging
import os
from collections.abc import (
    ItemsView, KeysView, Mapping, MappingView, Sequence, ValuesView
)

from typing import Union, List, Optional

import ruamel.yaml  # type: ignore
from ruamel.yaml import RoundTripRepresenter, YAML, yaml_object, RoundTripConstructor  # type: ignore
from ruamel.yaml.comments import CommentedMap  # type: ignore

from ymp.exceptions import YmpConfigError
from ymp.common import AttrDict


log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class LayeredConfError(YmpConfigError):
    """Error in LayeredConf"""
    def __init__(self, obj: object, msg: str, key: Optional[object]=None, stack = None):
        super().__init__(obj, msg, key)
        if stack:
            self.stack = stack

    def get_fileline(self):
        if self.obj:
            if hasattr(self.obj, "get_fileline"):
                return self.obj.get_fileline(self.key)
            if isinstance(self.obj, Sequence) and len(self.obj) == 2:
                if hasattr(self.obj[1], "_yaml_line_col"):
                    return self.obj[0], self.obj[1]._yaml_line_col.line
                else:
                    return self.obj
        return None, None

class Entry:
    def __init__(self, filename, yaml, index):
        self.filename = filename
        self.lineno = yaml._yaml_line_col.data[index][0] + 1


class MixedTypeError(LayeredConfError):
    """Mixed types in proxy collection"""


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
        except (IndexError, KeyError) as exc:
            raise AttributeError() from exc

    def __setattr__(self, key, value):
        try:
            if key[0] == "_":
                object.__setattr__(self, key, value)
            else:
                self[key] = value
        except (IndexError, KeyError) as exc:
            raise AttributeError() from exc

    def __delattr__(self, key):
        raise NotImplementedError()


class MultiProxy(object):
    """Base class for layered container structure"""
    def __init__(self, maps, root=None, parent=None, key=None):
        self._maps = list(maps)
        self._parent = parent
        self._key = key
        self._root = root

    def make_map_proxy(self, key, items):
        return MultiMapProxy(items, parent=self, key=key)

    def make_seq_proxy(self, key, items):
        return MultiSeqProxy(items, parent=self, key=key)

    def get_files(self):
        return [fn for fn, layer in self._maps]

    def get_linenos(self):
        return [layer._yaml_line_col.line
                for fn, layer in self._maps]

    def get_fileline(self, key = None):
        if key:
            for fname, layer in self._maps:
                if key in layer:
                    return fname, layer._yaml_line_col.data[key][0] + 1
        return ";".join(self.get_files()), next(iter(self.get_linenos()), None)

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
            raise LayeredConfError(self, f"in remove_layer: {map_name} != {name}")

    def _get_root(self):
        node = self
        while node._parent:
            node = node._parent
        return node

    def get_path(self, key=None, absolute=False):
        if isinstance(self[key], MultiProxy):
            return self[key].get_paths(absolute)

        for fn, layer in self._maps:
            try:
                path = layer[key]
            except KeyError:
                continue
            break
        else:
            return None

        rootpath = self._get_root()._root
        if isinstance(path, WorkdirTag):
            path = str(path)
            basepath = rootpath
        else:
            path = os.path.expanduser(path)
            basepath = os.path.dirname(fn)
        if os.path.isabs(path):
            return path
        filepath = os.path.join(basepath, path)
        if absolute:
            return os.path.normpath(filepath)
        return os.path.relpath(filepath, rootpath)


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
            stack = [Entry(fn, m, key) for fn, m in self._maps if key in m]
            raise MixedTypeError(
                self,
                f"Mixed data types for key '{key}'s in present in files",
                key = key,
                stack=stack
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
        for key in dict.fromkeys(k for _, m in self._maps for k in m):
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

    def get_paths(self, absolute=False):
        return AttrDict(
            (key, self.get_path(key, absolute) )
            for key in self.keys()
        )


class MultiSeqProxy(Sequence, MultiProxy, AttrItemAccessMixin):
    """Sequence Proxy for layered containers"""
    def __contains__(self, value):
        return any(value in m for _, m in self._maps)

    def _make_proxy(self, index, fn, item):
        if isinstance(item, Mapping):
            return self.make_map_proxy(index, [(fn, item)])
        if isinstance(item, str):
            return item
        if isinstance(item, Sequence):
            return self.make_seq_proxy(index, [(fn, item)])
        return item

    def __iter__(self):
        index = 0
        for fn, smap in self._maps:
            for item in smap:
                yield self._make_proxy(index, fn, item)
                index += 1

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
            except ValueError as exc:
                raise KeyError() from exc
        relindex = index
        for fn, smap in self._maps:
            if relindex >= len(smap):
                relindex -= len(smap)
            else:
                return self._make_proxy(index, fn, smap[relindex])
        else:
            raise IndexError()

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
        smap = self._maps[0][1]
        smap.extend(item)

    def get_paths(self, absolute=False):
        return [self.get_path(i, absolute) for i in range(len(self))]


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
            os.rename(outfile, outfile+".bkup")
            os.rename(outfile+".tmp", outfile)


RoundTripRepresenter.add_representer(LayeredConfProxy,
                                     RoundTripRepresenter.represent_dict)
RoundTripRepresenter.add_representer(MultiMapProxy,
                                     RoundTripRepresenter.represent_dict)
RoundTripRepresenter.add_representer(MultiSeqProxy,
                                     RoundTripRepresenter.represent_list)


rt_yaml = YAML(typ="rt")

@yaml_object(rt_yaml)
class WorkdirTag:
    yaml_tag = u"!workdir"

    def __init__(self, path) -> None:
        self.path = path

    def __repr__(self):
        return f"!workdir {self.path}"

    def __str__(self):
        return self.path

    @classmethod
    def from_yaml(cls, _constructor, node):
        return cls(node.value)

    @classmethod
    def to_yaml(cls, representer, instance):
        return representer.represent_scalar(
            cls.yaml_tag, instance.path
        )

def load(files, root=None):
    """Load configuration files

    Creates a `LayeredConfProxy` configuration object from a set of
    YAML files.

    Files listed later will override parts of earlier included files
    """

    def load_one(fname, stack):
        if any(fname == entry.filename for entry in stack):
            raise LayeredConfError((fname, None), "Recursion in includes", stack=stack)
        try:
            with open(fname, "r") as fdes:
                yaml = rt_yaml.load(fdes)
        except IOError as exc:
            raise LayeredConfError((fname, None), "Failed to read file", stack=stack) from exc
        if not isinstance(yaml, Mapping):
            raise LayeredConfError((fname, 1), "Config must have mapping as toplevel", stack=stack)
        layers = [(fname, yaml)]

        includes = yaml.get("include", [])
        if not includes:
            return layers

        basedir = os.path.dirname(fname)
        if isinstance(includes, str):
            path = os.path.join(basedir, includes)
            stack.append(Entry(fname, yaml, "include"))
            layers.extend(load_one(path, stack))
            stack.pop()
            return layers

        if not isinstance(includes, Sequence):
            raise LayeredConfError((fname, includes), 'Statement "include" must be a list', stack=stack)

        for num, include in enumerate(reversed(includes)):
            path = os.path.join(basedir, include)
            stack.append(Entry(fname, includes, num))
            layers.extend(load_one(path, stack))
            stack.pop()
        return layers

    layers = []
    for fname in reversed(files):
        layers.extend(load_one(str(fname), []))
    return LayeredConfProxy(layers, root=root)
