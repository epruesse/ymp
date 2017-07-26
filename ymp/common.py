from collections import OrderedDict


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
class _make_OrderedDict(object):
    def __getitem__(self, keys):
        return OrderedDict([(slice.start, slice.stop) for slice in keys])
odict = _make_OrderedDict()
