import logging
import re

from copy import copy, deepcopy

import networkx

from snakemake.exceptions import RuleException
from snakemake.io import AnnotatedString, apply_wildcards
from snakemake.io import Namedlist as _Namedlist
from snakemake.workflow import Workflow

import ymp
from ymp.common import flatten, is_container
from ymp.string import FormattingError, ProductFormatter, make_formatter


log = logging.getLogger(__name__)

partial_formatter = make_formatter(partial=True, quoted=True)
partial_format = partial_formatter.format
get_names = partial_formatter.get_names


class CircularReferenceException(RuleException):
    """Exception raised if parameters in rule contain a circular reference"""
    def __init__(self, deps, rule, include=None, lineno=None, snakefile=None):
        nodes = [n[0] for n in networkx.find_cycle(deps)]
        message = "Circular reference in rule {}:\n{}".format(
            rule, " => ".join(nodes + [nodes[0]]))
        super().__init__(message=message,
                         include=include,
                         lineno=lineno,
                         snakefile=snakefile,
                         rule=rule)


class NamedList(_Namedlist):
    """Extended version of Snakemake's io.namedlist

    - Fixes array assignment operator:
      Writing a field via `[]` operator updates the value accessed
      via `.` operator.
    - Adds `fromtuple` to constructor:
      Builds from Snakemake's typial `(args, kwargs)` tuples as
      present in ruleinfo structures.
    - Adds update_tuple method:
      Updates values in `(args,kwargs)` tuples as present in ruleinfo
      structures.
    """
    def __init__(self, fromtuple=None, **kwargs):
        super().__init__(**kwargs)
        self._fromtuple = fromtuple
        if fromtuple:
            for value in fromtuple[0]:
                self.append(value)
            for key, value in fromtuple[1].items():
                if is_container(value):
                    start = len(self)
                    for subvalue in value:
                        self.append(subvalue)
                    self.set_name(key, start, len(self))
                else:
                    self.append(value)
                    self.add_name(key)

    def __setitem__(self, idx, value):
        # set value in list
        super().__setitem__(idx, value)
        # set value in attributes (copied references)
        for name, (i, j) in self.get_names():
            if idx >= i and (j is None or idx < j):
                self.set_name(name, i, j)

    def update_tuple(self, totuple):
        """Update values in `(args, kwargs)` tuple.
        The tuple must be the same as used in the constructor and
        must not have been modified.
        """
        args, kwargs = totuple
        for n, value in enumerate(args):
            if args[n] != self[n]:
                args[n] = self[n]
        for key, value in kwargs.items():
            start, end = self._names[key]
            if end:
                assert is_container(value)
                for k, j in enumerate(range(start, end)):
                    if kwargs[key][k] != self[j]:
                        kwargs[key][k] = self[j]
            else:
                if kwargs[key] != self[start]:
                    kwargs[key] = self[start]


ruleinfo_fields = {
    'wildcard_constraints': {
        'format': 'argstuple',  # len(t[0]) must be == 0
    },
    'input': {
        'format': 'argstuple',
        'funcparams': ('wildcards',),
        'apply_wildcards': True,
    },
    'output': {
        'format': 'argstuple',
        'apply_wildcards': True,
    },
    'threads': {
        'format': 'int',
        'funcparams': ('input', 'attempt', 'threads')
        # stored as resources._cores
    },
    'resources': {
        'format': 'argstuple',  # len(t[0]) must be == 0, t[1] must be ints
        'funcparams': ('input', 'attempt', 'threads'),
    },
    'params': {
        'format': 'argstuple',
        'funcparams': ('wildcards', 'input', 'resources', 'output', 'threads'),
        'apply_wildcards': True,
    },
    'shadow_depth': {
        'format': 'string_or_true',
    },
    'priority': {
        'format': 'numeric',
    },
    'version': {
        'format': 'object',
    },
    'log': {
        'format': 'argstuple',
        'apply_wildcards': True,
    },
    'message': {
        'format': 'string',
        'format_wildcards': True,
    },
    'benchmark': {
        'format': 'string',
        'apply_wildcards': True,
    },
    'wrapper': {
        'format': 'string',
        # sets conda_env
    },
    'conda_env': {
        'format': 'string',  # path, relative to cwd or abs
        'apply_wildcards': True,
        # works only with shell/script/wrapper, not run
    },
    'singularity_img': {
        'format': 'string',
        # works ony with shell/script/wrapper, not run
    },
    'shellcmd': {
        'format': 'string',
        'format_wildcards': True
    },
    'docstring': {
        'format': 'string',
    },
    # func
    # norun
    # script
    # restart_times
}


def recursive_format(rule, ruleinfo):
    """Expand wildcards within ruleinfo
    """
    excluded_fields = (
        'shellcmd',
        'message',
        'wildcard_constraints'
    )
    fields = [field for field in ruleinfo_fields.keys()
              if field not in excluded_fields
              if getattr(ruleinfo, field) is not None]

    # normalize field values and create namedlist dictionary
    args = {}
    for field in fields:
        attr = getattr(ruleinfo, field)
        if isinstance(attr, tuple):
            if len(attr) != 2:
                raise Exception("Internal Error")
            # flatten named lists
            for key in attr[1]:
                if is_container(attr[1][key]):
                    attr[1][key] = list(flatten(attr[1][key]))
            # flatten unnamed and overwrite tuples
            # also turn attr[0] into a list, making it mutable
            attr = (list(flatten(attr[0])), attr[1])

            setattr(ruleinfo, field, attr)
            args[field] = NamedList(fromtuple=attr)
        else:
            args[field] = NamedList()
            args[field].append(attr)

    # build graph of expansion dependencies
    deps = networkx.DiGraph()
    for field, nlist in args.items():
        for n, value in enumerate(nlist):
            if not isinstance(value, str):  # only strings can be expanded
                continue
            s = "{}[{}]".format(field, n)
            # create node for value itself
            deps.add_node(s, core=True, name=field, idx=n)
            # node depends on wildcards contained in value
            deps.add_edges_from((s, t)
                                for t in get_names(value)
                                if t.split(".")[0].split("[")[0] in fields)
            # field node depends on all it's value nodes
            deps.add_edge(field, s)
        # create edges field.name -> field[n]
        for name, (i, j) in nlist.get_names():
            s = "{}.{}".format(field, name)
            if j is None:
                j = i + 1
            deps.add_edges_from((s, "{}[{}]".format(field, n))
                                for n in range(i, j))

    # sort variables so that they can be expanded in order
    try:
        nodes = list(reversed([
            node for node in networkx.algorithms.dag.topological_sort(deps)
            if deps.out_degree(node) > 0 and 'core' in deps.nodes[node]
        ]))
    except networkx.NetworkXUnfeasible:
        raise CircularReferenceException(deps, rule)

    # expand variables
    for node in nodes:
        name = deps.nodes[node]['name']
        idx = deps.nodes[node]['idx']
        value = args[name][idx]
        if isinstance(value, str):
            try:
                value2 = partial_format(value, **args)
            except FormattingError as e:
                raise RuleException(
                    "Unable to resolve wildcard '{{{}}}' in parameter {} in"
                    "rule {}".format(e.attr, node, rule.name))
            args[name][idx] = value2
            if ymp.print_rule == 1:
                log.error("{}::{}: {} => {}".format(rule.name,
                                                    node, value, value2))

    # update ruleinfo
    for name in fields:
        attr = getattr(ruleinfo, name)
        if isinstance(attr, tuple):
            if len(attr) != 2:
                raise Exception("Internal Error")
            args[name].update_tuple(attr)
        else:
            setattr(ruleinfo, name, args[name][0])


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
