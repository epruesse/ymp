import logging
import re

from collections import Iterable
from copy import copy, deepcopy

from snakemake.io import AnnotatedString, Namedlist, apply_wildcards
from snakemake.workflow import Workflow

import ymp
from ymp.string import PartialFormatter, ProductFormatter


log = logging.getLogger(__name__)

partial_format = PartialFormatter().format


def flatten(l):
    """Flatten lists without turning strings into letters"""
    for item in l:
        if isinstance(item, str):
            yield item
        elif isinstance(item, Iterable):
            for item2 in flatten(item):
                yield item2
        else:
            yield item


def recursive_format(rule, ruleinfo):
    """Expand wildcards within ruleinfo

    This is not fully implemented!

    At this time, only `input` and `output` are expanded within
    `params`.
    """
    args = {}
    for name in ['input', 'output']:
        attr = getattr(ruleinfo, name)
        if attr is None:
            continue
        nlist = Namedlist()
        for item in flatten(attr[0]):
            nlist.append(item)
        for key, item in attr[1].items():
            nlist.append(item)
            nlist.add_name(key)
        args[name] = nlist
    for key, value in ruleinfo.params[1].items():
        if not isinstance(value, str):
            continue
        value = partial_format(value, **args)
        ruleinfo.params[1][key] = value


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
                workflow._ruleinfos = {}
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

    def _apply_expand_funcs(self, name, paths, kwpaths):
        if name in self.expand_funcs:
            for func in reversed(self.expand_funcs[name]):
                try:
                    (paths, kwpaths) = func(paths, kwpaths)
                except Exception as e:
                    log.exception("exception in input expand")
        return paths, kwpaths

    def input(self, *paths, **kwpaths):
        """Intercepts arguments passed to "rule: input:" and passes them
        through registered expander functions.
        """
        paths, kwpaths = self._apply_expand_funcs('input', paths, kwpaths)
        return super().input(*paths, **kwpaths)

    def output(self, *paths, **kwpaths):
        """Intercepts arguments passed to "rule: output:" and passes them
        through registered expander functions.
        """
        paths, kwpaths = self._apply_expand_funcs('output', paths, kwpaths)
        return super().output(*paths, **kwpaths)

    @staticmethod
    def default_params(**kwargs):
        """Set default params: keys"""
        ExpandableWorkflow._default_params = kwargs

    def add_rule(self, name=None, lineno=None, snakefile=None):
        """Add a rule.

        Arguments:
          name: name of the rule
          lineno: line number within the snakefile where the rule was defined
          snakefile: name of file in which rule was defined
        """
        # super().add_rule() dynamically creates a name if `name` is None
        # stash the name so we can access it from `get_rule`
        self._last_rule_name = super().add_rule(name, lineno, snakefile)
        return self._last_rule_name

    def get_rule(self, name=None):
        """
        Get rule by name. If name is none, the last created rule is returned.

        Arguments:
          name: the name of the rule
        """
        if name is None:
            name = self._last_rule_name
        return super().get_rule(name)

    def rule(self, name=None, lineno=None, snakefile=None):
        """Intercepts "rule:"
        Here we have the entire ruleinfo object
        """
        decorator = super().rule(name, lineno, snakefile)
        rule = self.get_rule(name)

        def decorate(ruleinfo):
            # save original ruleinfo in case `derive_rule` is called
            self._ruleinfos[name] = ruleinfo

            # if we have default params, add them
            if self._default_params:
                if not ruleinfo.params:
                    ruleinfo.params = ([], {})
                for param in self._default_params:
                    if param not in ruleinfo.params[1]:
                        ruleinfo.params[1][param] = self._default_params[param]

            recursive_format(rule, ruleinfo)

            # Conditionaly dump rule after YMP formatting
            if ymp.print_rule == 1:
                log.error("rule {}".format({'n': name,
                                            'l': lineno,
                                            's': snakefile}))
                for attr in dir(ruleinfo):
                    if attr.startswith("__"):
                        continue
                    log.error("  {}: {}".format(attr,
                                                getattr(ruleinfo, attr, "")))
                ymp.print_rule = 0
            # register rule with snakemake
            decorator(ruleinfo)  # does not return anything

        return decorate

    def derive_rule(self, name, parent, order=None, **kwargs):
        """Create derived snakemake rule by overriding rule parameters

        This implements a poor man's OO solution for Snakemake. By overwriting
        parts of the rule, we can create several similar rules without too
        much repetition.

        This is mainly necessary for alternative output scenarios, e.g. if
        the output can be paired end or single end, snakemake syntax does
        not support expressing this in one rule.

        Arguments:
           name: name of derived rule
           parent: name of parent rule
           order: one of "lesser" or "higher"; creates ruleorder statement
           input, output, params, ...: override parent arguments

        The active part, ``shell`, ``run``, ``script``, etc. cannot be
        overriden.

        String and list parameters override the unnamed arguments.
        Dict arguments override named arguments with ``dict.update`` behavior.
        Tuples are expected to contain an array *args and a dict *kwargs,
        overriding as above.
        """
        ruleinfo = deepcopy(self._ruleinfos[parent])
        # log.error("deriving {} from {}".format(name, parent))

        for key, value in kwargs.items():
            attr = getattr(ruleinfo, key)
            if isinstance(value, str):
                value, _ = self._apply_expand_funcs(key, [value], {})
                attr = (value, attr[1])
            elif isinstance(value, list):
                value, _ = self._apply_expand_funcs(key, value, {})
                attr = (value, attr[1])
            elif isinstance(value, dict):
                _, value = self._apply_expand_funcs(key, [], value)
                attr[1].update(value)
            elif isinstance(value, tuple):
                value = self._apply_expand_funcs(key, value[0], value[1])
                attr[0] = value[0]
                attr[1].update(value[1])

            setattr(ruleinfo, key, attr)

        self.rule(name=name, snakefile="generated")(ruleinfo)

        if order is not None:
            if order == "lesser":
                self.ruleorder(parent, name)
            elif order == "higher":
                self.ruleorder(name, parent)


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
            raise ValueError("unable to expand item '{}' with fields '{}'"
                             "".format(repr(item),
                                       repr(fields)))

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
