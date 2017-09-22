import logging
import re

from copy import deepcopy, copy

from snakemake.io import AnnotatedString, apply_wildcards
from snakemake.workflow import Workflow

from ymp.string import ProductFormatter

log = logging.getLogger(__name__)


class ExpandableWorkflow(Workflow):
    """Adds hook for additional wildcard expansion methods to Snakemake"""
    @staticmethod
    def activate():
        """Installs the ExpandableWorkflow

        Replaces the Workflow object in the snakemake.workflow module
        with an instance of this class and initializes default expanders
        (the snakemake syntax).
        """
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
            log.debug("ExpandableWorkflow not installed: "
                      "Failed to import workflow object.")

    @staticmethod
    def register_expandfuncs(expand_input=None, expand_output=None):
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
        """Intercepts arguments passed to "rule: input:" and passes them
        through registered expander functions.
        """
        for func in reversed(self.expand_funcs['input']):
            try:
                (paths, kwpaths) = func(paths, kwpaths)
            except Exception as e:
                log.exception("exception in input expand")
        return super().input(*paths, **kwpaths)

    def output(self, *paths, **kwpaths):
        """Intercepts arguments passed to "rule: output:" and passes them
        through registered expander functions.
        """
        for func in reversed(self.expand_funcs['output']):
            try:
                (paths, kwpaths) = func(paths, kwpaths)
            except Exception as e:
                log.exception("exception in output expand")
        return super().output(*paths, **kwpaths)


class BaseExpander(object):
    def __init__(self):
        ExpandableWorkflow.register_expandfuncs(
            expand_input=self.expand_input,
            expand_output=self.expand_output
        )

    def format(self, item, *args, **kwargs):
        return item

    def expand(self, item, fields, rec=-1):
        if isinstance(item, str):
            updated = self.format(item, **fields)
            if isinstance(item, AnnotatedString):
                updated = AnnotatedString(updated)
                try:
                    updated.flags = deepcopy(item.flags)
                except TypeError as e:
                    log.debug(
                        "Failed to deepcopy flags for item {} with flags{}"
                        "".format(item, item.flags)
                    )
                    updated.flags = copy(item.flags)
            item = updated
        elif hasattr(item, '__call__'):  # function
            _item = item

            def late_expand(*args, **kwargs):
                return self.expand(_item(*args, **kwargs),
                                   {'wc': args[0]}, rec=rec+1)
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
            return (self.expand(subitem, fields, rec=rec+1)
                    for subitem in item)
        else:
            raise ValueError("unable to expand item '{}'".format(repr(item)))

        return item

    def expand_input(self, paths, kwpaths):
        def make_l(path):
            return lambda wc: self.expand(path, {'wc': wc})

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
        paths = [self.expand(path, {}) for path in paths]
        kwpaths = {key: self.expand(path, {}) for key, path in kwpaths.items()}
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

    class Formatter(ProductFormatter):
        def __init__(self, expander):
            self.expander = expander
            super().__init__()

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

    def get_names(self, pattern):
        return set(match.group('name')
                   for match in self._regex.finditer(pattern))


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
