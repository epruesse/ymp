from itertools import product
from string import Formatter


class OverrideJoinFormatter(Formatter):
    """Formatter with overridable join method

    The default formatter joins all arguments with `"".join(args)`. This
    class overrides _vformat with identical code, changing only that line
    to one that can be overridden by a derived class.
    """

    def _vformat(self, format_string, args, kwargs, used_args, recursion_depth,
                 auto_arg_index=0):
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

                # format the object and append to the result
                result.append(obj)

        return self.join(result), auto_arg_index

    def join(self, args):
        """
        Joins the expanded pieces of the template string to form the output.

        This function is equivalent to `''.join(args)`. By overriding it,
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

    >>> "{A} and {B}".format(A=[1,2],B=[3,4])
    "1 and 3"
    "1 and 4"
    "2 and 3"
    "2 and 4"
    """
    def join(self, args):
        # expand everything that isn't a string to a list
        args = [[item] if isinstance(item, str) else list(item)
                for item in args]
        # combine items into list corresponding to cartesian product
        res = [''.join(flat_args) for flat_args in product(*args)]

        # only one result? just return the one without list
        if len(res) == 1:
            res = res[0]
        # no results at all? expand to ''
        if len(res) == 0:
            return ''
        return res


class RegexFormatter(Formatter):
    """
    String Formatter accepting a regular expression defining the format
    of the expanded tags.
    """
    def __init__(self, regex):
        super().__init__()
        self._regex = regex

    def parse(self, format_string):
        if format_string is None:
            return

        start = 0
        for match in self.expander._regex.finditer(format_string):
            yield (format_string[start:match.start()],
                   match.group('name'), '', None)
            start = match.end()

        yield (format_string[start:],
               None, None, None)


class PartialFormatter(Formatter):
    def get_value(self, key, args, kwargs):
        if isinstance(key, str):
            return kwargs.get(key,  # key in kwards
                              "{{{}}}".format(key))  # key not found
        else:
            return super().get_value(key, args, kwargs)
