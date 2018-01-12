from collections import Mapping, OrderedDict


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


odict = OrderedDictMaker()  # need only one instance ever


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
