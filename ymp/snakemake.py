from snakemake.workflow import Workflow
from snakemake.io import expand, apply_wildcards, AnnotatedString
from string import Formatter
from itertools import product
from copy import deepcopy
import re, os, csv

class OverrideJoinFormatter(Formatter):
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
                #result.append(self.format_field(obj, format_spec))
                result.append(obj)

        return self.join(result), auto_arg_index

    def join(self, args):
        return ''.join(args)


class ExpandableWorkflow(Workflow):
    @staticmethod
    def activate():
        try:
            from snakemake.workflow import workflow

            if workflow.__class__ != ExpandableWorkflow:
                workflow.__class__ = ExpandableWorkflow
                workflow.expand_funcs = {
                    'input': [
                    ],
                    'output': [
                    ]
                }
                workflow.sm_expander = SnakemakeExpander()
        except ImportError:
            pass

    @staticmethod
    def register_expandfuncs(expand_input = None, expand_output = None):
        ExpandableWorkflow.activate()
        try:
            from snakemake.workflow import workflow

            if expand_input:
                workflow.expand_funcs['input'].append(expand_input)
            if expand_output:
                workflow.expand_funcs['output'].append(expand_output)
        except ImportError:
            pass

    def input(self, *paths, **kwpaths):
        for func in reversed(self.expand_funcs['input']):
            try:
                (paths, kwpaths) = func(paths, kwpaths)
            except Exception as e:
                print("exception in input expand:" + repr(e))
        return super().input(*paths, **kwpaths)

    def output(self, *paths, **kwpaths):
        for func in reversed(self.expand_funcs['output']):
            try:
                (paths, kwpaths) = func(paths, kwpaths)
            except Exception as e:
                print("exception in output expand:" + repr(e))
        return super().output(*paths, **kwpaths)


class BaseExpander(object):
    def __init__(self):
        ExpandableWorkflow.register_expandfuncs(
            expand_input = self.expand_input,
            expand_output = self.expand_output
        )
        
    def format(item, *args, **kwargs):
        return item
    
    def expand(self, item, fields, rec=-1):
        if isinstance(item, str):
            updated = self.format(item, **fields)
            if isinstance(item, AnnotatedString):
                updated = AnnotatedString(updated)
                updated.flags = deepcopy(item.flags)
            item = updated
        elif hasattr(item, '__call__'):  # function
            _item = item
            def late_expand(*args, **kwargs):
                return self.expand(_item(*args, **kwargs), {'wc':args[0]}, rec=rec+1)
            item = late_expand
        elif isinstance(item, int):
            pass
        elif isinstance(item, dict):
            for key, value in item.items():
                item[key] = self.expand(value, fields, rec=rec+1)
        elif isinstance(item, list):
            for i, subitem in enumerate(item):
                item[i] = self.expand(subitem, fields, rec=rec+1)
        elif isinstance(item, tuple):
            return (self.expand(subitem, fields, recurse) for subitem in item)
        else:
            raise ValueError("unable to expand item '{}'".format(repr(item)))

        return item

    def expand_input(self, paths, kwpaths):
        def make_l(path):
            return lambda wc: self.expand(path, {'wc':wc})

        new_paths = []
        for path in paths:
            try:
                new_paths.append(self.expand(path, {}))
            except KeyError:
                new_paths.append(make_l(path))
                
        new_kwpaths = {}
        for key, path in kwpaths.items():
            try:
                new_kwpaths[key] = self.expand(path, {})
            except KeyError:
                new_kwpaths[key] = make_l(path)
                

        return new_paths, new_kwpaths

    def expand_output(self, paths, kwpaths):
        paths = [self.expand(path,{}) for path in paths]
        kwpaths = {key:self.expand(path,{}) for key, path in kwpaths.items() }
        return paths, kwpaths


class SnakemakeExpander(BaseExpander):
    def format(self, item, *args, **kwargs):
        if 'wc' in kwargs:
            return apply_wildcards(item, kwargs['wc'])
        return item


class FormatExpander(BaseExpander):
    _regex = re.compile(
        r"""
        \{
            (?=(
                (?P<name>[^{}]+)
            ))\1
        \}
        """, re.VERBOSE)

    def __init__(self):
        super().__init__()
        self.formatter = self.Formatter(self)

    def format(self, *args, **kwargs):
        return self.formatter.format(*args, **kwargs)
            
    class Formatter(OverrideJoinFormatter):
        def __init__(self, expander):
            self.expander = expander
            super().__init__()
            
        def parse(self, format_string):
            if format_string is None:
                return

            start = 0
            for match in self.expander._regex.finditer(format_string):
                yield (format_string[start:match.start()], match.group('name'), '', None)
                start = match.end()

            yield (format_string[start:], None, None, None)

        def join(self, args):
            args = [[item] if isinstance(item, str) else list(item) for item in args]
            res =  [''.join(flat_args) for flat_args in product(*args)]
            if len(res) == 1:
                res = res[0]
            if len(res) == 0:
                return ''
            return res

    def get_names(self, pattern):
        return set(match.group('name') for match in self._regex.finditer(pattern))


class ColonExpander(FormatExpander):
    _regex = re.compile(
        r"""
        \{:
            (?=(
                \s*
                 (?P<name>(?:.(?!\s*\:\}))*.)
                \s*
            ))\1
        :\}
        """, re.VERBOSE)

    def __init__(self):
        super().__init__()
