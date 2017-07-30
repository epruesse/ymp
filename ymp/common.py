from collections import OrderedDict, Mapping

class _odict(object):
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
        return OrderedDict([(slice.start, slice.stop) for slice in keys])

odict = _odict() # need only one instance ever


def update_dict(dst, src):#
    """Recursively update dictionary `dst` with `src`

    Treats a `list` as atomic, replacing it with new list.
    """
    for key, val in src.items():
        if isinstance(val, Mapping):
            tmp = update_dict(dst.get(key, {}), val)
            dst[key] = tmp
        else:
            dst[key] = src[key]
    return dst
