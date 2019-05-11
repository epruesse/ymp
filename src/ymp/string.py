import re

from itertools import product
from string import Formatter
from typing import List, Dict, Tuple, Union, Any, Set

import snakemake.utils


class FormattingError(AttributeError):
    def __init__(self, message: str, fieldname: str) -> None:
        super().__init__(message)
        self.attr = fieldname


class GetNameFormatter(Formatter):
    def get_names(self, pattern: str):
        for val in self.parse(pattern):
            if val[1] is not None:
                yield val[1]


class OverrideJoinFormatter(Formatter):
    """Formatter with overridable join method

    The default formatter joins all arguments with
    ``"".join(args)``. This class overrides :meth:`_vformat` with
    identical code, changing only that line to one that can be
    overridden by a derived class.
    """

    def _vformat(self,
                 format_string: str,
                 args: List,
                 kwargs: Dict,
                 used_args,
                 recursion_depth: int,
                 auto_arg_index: int=0) -> Tuple[Union[List[str], str], int]:
        if recursion_depth < 0:
            raise ValueError('Max string recursion exceeded')
        result = []
        for literal_text, field_name, format_spec, conversion in \
                self.parse(format_string):

            # output the literal text
            if literal_text:
                result.append(literal_text)

            # if there's a field, output it
            if field_name is not None:
                # this is some markup, find the object and do
                #  the formatting

                # handle arg indexing when empty field_names are given.
                if field_name == '':
                    if auto_arg_index is False:
                        raise ValueError('cannot switch from manual field '
                                         'specification to automatic field '
                                         'numbering')
                    field_name = str(auto_arg_index)
                    auto_arg_index += 1
                elif field_name.isdigit():
                    if auto_arg_index:
                        raise ValueError('cannot switch from manual field '
                                         'specification to automatic field '
                                         'numbering')
                    # disable auto arg incrementing, if it gets
                    # used later on, then an exception will be raised
                    auto_arg_index = False

                # given the field_name, find the object it references
                #  and the argument it came from
                obj, arg_used = self.get_field(field_name, args, kwargs)
                used_args.add(arg_used)

                # do any conversion on the resulting object
                obj = self.convert_field(obj, conversion)

                # expand the format spec, if needed
                format_spec, auto_arg_index = self._vformat(
                    format_spec, args, kwargs,
                    used_args, recursion_depth-1,
                    auto_arg_index=auto_arg_index)

                result.append(self.format_field(obj, format_spec))

        return self.join(result), auto_arg_index

    def join(self, args: List[str]) -> Union[List[str],str]:
        """
        Joins the expanded pieces of the template string to form the output.

        This function is equivalent to ``''.join(args)``. By overriding it,
        alternative methods can be implemented, e.g. to create a list of
        strings, each corresponding to a the cross product of the expanded
        variables.
        """
        return ''.join(args)


class ProductFormatter(OverrideJoinFormatter):
    """
    String Formatter that creates a list of strings each expanded using
    one point in the cartesian product of all replacement values.

    If none of the arguments evaluate to lists, the result is a string,
    otherwise it is a list.

    >>> ProductFormatter().format("{A} and {B}", A=[1,2], B=[3,4])
    "1 and 3"
    "1 and 4"
    "2 and 3"
    "2 and 4"
    """
    def join(self, args: List[Any]) -> Union[List[str], str]:
        # expand everything that isn't a string to a list
        args = [[item] if isinstance(item, str) else list(item)
                for item in args]
        # combine items into list corresponding to cartesian product
        res = [''.join(flat_args) for flat_args in product(*args)]

        if len(res) > 1:
            return res

        if res:
            return res[0]

        return ''

    def format_field(self, value, format_spec: str):
        if hasattr(value, '__iter__') and not isinstance(value, str):
            return (format(item) for item in value)
        return format(value, format_spec)


class RegexFormatter(Formatter):
    """
    String Formatter accepting a regular expression defining the format
    of the expanded tags.
    """
    def __init__(self, regex: Union[str, Any]) -> None:
        super().__init__()
        if (isinstance(regex, str)):
            self.regex = re.compile(regex)
        else:
            self.regex = regex

    def parse(self, format_string: str):
        """
        Parse format_string into tuples. Tuples contain
        literal_text: text to copy
        field_name: follwed by field name
        format_spec:
        conversion:
        """
        if format_string is None:
            return

        start = 0
        for match in self.regex.finditer(format_string):
            yield (format_string[start:match.start()],  # literal text
                   match.group('name'),                 # field name
                   '',                                  # format spec
                   None)                                # conversion
            start = match.end()

        # yield text at end of format_string
        yield (format_string[start:],
               None, None, None)

    def get_names(self, format_string: str) -> Set[str]:
        """Get set of field names in format_string)"""
        return set(match.group('name')
                   for match in self.regex.finditer(format_string))


class QuotedElementFormatter(snakemake.utils.SequenceFormatter):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.element_formatter = snakemake.utils.QuotedFormatter()


class PartialFormatter(Formatter):
    """
    Formats what it can and leaves the remainder untouched
    """

    def get_field(self, field_name, args, kwargs):
        try:
            val = super().get_field(field_name, args, kwargs)
            if type(val[0]).__name__ == "function":
                raise IndexError()
            return val
        except (KeyError, IndexError, TypeError):
            return getattr(self, "spec", "{{{}}}").format(field_name), None


def make_formatter(product=None, regex=None, partial=None, quoted=None):
    formatter = 1
    types: 'List[type]' = []
    class_name = ""
    class_dict = {}
    for arg, cls, name in (
            (product, ProductFormatter, 'Product'),
            (regex, RegexFormatter, 'Regex'),
            (partial, PartialFormatter, 'Partial'),
            (quoted, QuotedElementFormatter, 'QuotedElement'),
            (formatter, GetNameFormatter, 'Formatter')
             ):
        if arg is not None:
            types += [cls]
            class_name += name
            if not isinstance(arg, int):
                class_dict[name.lower()] = arg
    return type(class_name, tuple(types), {})(**class_dict)
