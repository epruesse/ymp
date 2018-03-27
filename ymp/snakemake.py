"""
Extends Snakemake Features
"""

import functools
import logging
import sys
import re
from copy import copy, deepcopy

from typing import Optional


from snakemake.exceptions import RuleException
from snakemake.io import AnnotatedString, apply_wildcards
from snakemake.io import Namedlist as _Namedlist
from snakemake.rules import Rule
from snakemake.workflow import RuleInfo, Workflow

import ymp
from ymp.common import flatten, is_container
from ymp.string import FormattingError, ProductFormatter, make_formatter


log = logging.getLogger(__name__)  # pylint: disable=invalid-name


def networkx():
    import networkx
    if networkx.__version__[0] != "2":
        log.fatal("Networkx version 2.* required by YMP but {} found"
                  "".format(networkx.__version__))
        sys.exit(1)
    return networkx


partial_formatter = make_formatter(partial=True, quoted=True)
partial_format = partial_formatter.format
get_names = partial_formatter.get_names


def print_ruleinfo(rule: Rule, ruleinfo: RuleInfo, func=log.debug):
    """Logs contents of Rule and RuleInfo objects.

    Arguments:
      rule: Rule object to be printed
      ruleinfo: Matching RuleInfo object to be printed
      func: Function used for printing (default is log.error)
    """
    func("rule {}".format({'n': rule.name,
                           'l': rule.lineno,
                           's': rule.snakefile}))
    for attr in dir(ruleinfo):
        if attr.startswith("__"):
            continue
        func("  {}: {}".format(attr,
                               getattr(ruleinfo, attr, "")))
    func(ruleinfo.func.__code__)


def load_workflow(snakefile=ymp._snakefile):
    workflow = ExpandableWorkflow(snakefile=snakefile)
    workflow.include(snakefile)
    return workflow


def get_workflow():
    workflow = ExpandableWorkflow.global_workflow
    if workflow is None:
        ExpandableWorkflow.activate()
        workflow = ExpandableWorkflow.global_workflow
        if workflow is None:
            workflow = load_workflow()
    return workflow


class CircularReferenceException(RuleException):
    """Exception raised if parameters in rule contain a circular reference"""
    def __init__(self, deps, rule, include=None, lineno=None, snakefile=None):
        nodes = [n[0] for n in networkx().find_cycle(deps)]
        message = "Circular reference in rule {}:\n{}".format(
            rule, " => ".join(nodes + [nodes[0]]))
        super().__init__(message=message,
                         include=include,
                         lineno=lineno,
                         snakefile=snakefile,
                         rule=rule)


class InheritanceException(RuleException):
    """Exception raised for errors during rule inheritance"""
    def __init__(self, msg, rule, parent,
                 include=None, lineno=None, snakefile=None):
        message = "'{}' when deriving {} from {}".format(
            msg, rule.name, parent)
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


#: describes attributes of :py:class:`snakemake.workflow.RuleInfo`
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
    'norun': {  # does the rule have executable data?
        'format': 'bool',
    },
    'func': {
        'format': 'callable',
    },
    'script': {
        'format': 'string',
    }
    # restart_times
}


class ExpandableWorkflow(Workflow):
    """Adds hook for additional rule expansion methods to Snakemake"""
    global_workflow = None

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
                ExpandableWorkflow.global_workflow = workflow
                ExpandableWorkflow.global_workflow.__init__()

        except ImportError:
            log.debug("ExpandableWorkflow not installed: "
                      "Failed to import workflow object.")

    def __init__(self, *args, **kwargs):
        """Constructor for ExpandableWorkflow overlay attributes

        This is called on an already initialized Workflow object.
        """
        if not self.__dict__:
            # only call constructor if this object hasn't been initialized yet
            super().__init__(*args, **kwargs)
            ExpandableWorkflow.global_workflow = self
        self._expanders = []
        self._sm_expander = SnakemakeExpander()
        self._ruleinfos = {}
        self._last_rule_name = None

    @staticmethod
    def register_expander(expander):
        """
        Register an object the expand() function of which will be called
        on each RuleInfo object before it is passed on to snakemake.
        """
        ExpandableWorkflow.activate()
        if ExpandableWorkflow.global_workflow:
            workflow = ExpandableWorkflow.global_workflow
            workflow._expanders.append(expander)
            return workflow
        return None

    @staticmethod
    def clear():
        if ExpandableWorkflow.global_workflow:
            ExpandableWorkflow.global_workflow.__init__()

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

            if ymp.print_rule == 1:
                log.error("#### BEGIN expansion")
                print_ruleinfo(rule, ruleinfo, log.error)
                rule._ymp_print_rule = True

            for expander in reversed(self._expanders):
                expander.expand(rule, ruleinfo)
                if ymp.print_rule == 1:
                    log.error("### expanded with " + type(expander).__name__)
                    print_ruleinfo(rule, ruleinfo, log.error)
            if ymp.print_rule:
                log.error("#### END expansion")

            # Conditionaly dump rule after YMP formatting
            if ymp.print_rule == 1:
                ymp.print_rule = 0

            # register rule with snakemake
            try:
                decorator(ruleinfo)  # does not return anything
            except AttributeError:
                print_ruleinfo(rule, ruleinfo, log.error)
                raise

        return decorate


def make_rule(name: str=None, lineno: int=None, snakefile: str=None,
              **kwargs):
    log.debug("Synthesizing rule {}".format(name))
    ruleinfo = RuleInfo(lambda: None)
    for arg in kwargs:
        setattr(ruleinfo, arg, kwargs[arg])
    ruleinfo.norun = True
    workflow = get_workflow()
    return workflow.rule(name, lineno, snakefile)(ruleinfo)


class BaseExpander(object):
    """
    Base class for Snakemake expansion modules.

    Subclasses should override the :meth:expand method if they need to
    work on the entire RuleInfo object or the :meth:format and
    :meth:expands_field methods if they intend to modify specific fields.
    """
    def __init__(self):
        self.workflow = ExpandableWorkflow.register_expander(self)

    def format(self, item, *args, **kwargs):
        """Format *item* using *\*args* and *\*\*kwargs*"""
        return item

    def format_annotated(self, item, expand_args):
        """Wrapper for :meth:format preserving *AnnotatedString* flags

        Calls :meth:format to format *item* into a new string and copies
        flags from original item.

        This is used by :meth:expand
        """
        updated = self.format(item, **expand_args)
        if isinstance(item, AnnotatedString):
            updated = AnnotatedString(updated)
            try:
                updated.flags = deepcopy(item.flags)
            except TypeError:
                log.debug(
                    "Failed to deepcopy flags for item {} with flags{}"
                    "".format(item, item.flags)
                )
            updated.flags = copy(item.flags)
        return updated

    def expands_field(self, field):
        """Checks if this expander should expand a Rule field type

        Arguments:
           field: the field to check
        Returns:
           True if *field* should be expanded.
        """
        return False

    def expand(self, rule, item, expand_args=None, rec=-1, cb=False):
        """Expands RuleInfo object and children recursively.

        Will call :meth:format (via :meth:format_annotated) on `str` items
        encountered in the tree and wrap encountered functions to be called
        once the wildcards object is available.

        Set `ymp.print_rule = 1` before a `rule:` statement in snakefiles
        to enable debug logging of recursion.

        Arguments:
          rule: The :class:snakemake.rules.Rule object to be populated with
            the data from the RuleInfo object passed from *item*
          item: The item to be expanded. Initially a
            :class:snakemake.workflow.RuleInfo object into which is recursively
            decendet. May ultimately be `None`, `str`, `function`, `int`,
            `float`, `dict`, `list` or `tuple`.
          expand_args: Parameters passed on late expansion (when the `dag`
            tries to instantiate the `rule` into a `job`.
          rec: Recursion level
        """
        rec = rec + 1
        if expand_args is None:
            expand_args = {}
        debug = ymp.print_rule or getattr(rule, "_ymp_print_rule", False)
        if debug:
            log.debug("{}{} {} {} in rule {} with args {}"
                      "".format(" "*rec*4, type(self).__name__,
                                type(item).__name__, item, rule, expand_args))
        if item is None:
            item = None
        elif isinstance(item, RuleInfo):
            for field in filter(self.expands_field, ruleinfo_fields):
                attr = getattr(item, field)
                setattr(item, field, self.expand(rule, attr,
                                                 expand_args=expand_args,
                                                 rec=rec))
        elif isinstance(item, str):
            try:
                expand_args['rule'] = rule
                item = self.format_annotated(item, expand_args)
            except KeyError:
                if cb:
                    # we already are being called: fail
                    raise
                # try expanding once we have wildcards
                _item = item

                def item(wc):
                    return self.expand(rule, _item,
                                       expand_args={'wc': wc, 'rule': rule},
                                       cb=True)
        elif hasattr(item, '__call__'):
            # continue expansion of function later by wrapping it
            _item = item

            @functools.wraps(item)
            def late_expand(*args, **kwargs):
                if debug:
                    log.debug("{}{} late {} {} "
                              "".format(" "*rec*4, type(self).__name__,
                                        args, kwargs))
                res = self.expand(rule, _item(*args, **kwargs),
                                  expand_args={'wc': args[0]},
                                  rec=rec,
                                  cb=True)
                if debug:
                    log.debug("{}=> {}"
                              "".format(" "*rec*4, res))
                return res
            item = late_expand
        elif isinstance(item, int) or isinstance(item, float):
            pass
        elif isinstance(item, dict):
            for key, value in item.items():
                _item = self.expand(rule, value, expand_args=expand_args,
                                    rec=rec)

                # Snakemake can't have functions in lists in dictionaries.
                # Let's fix that, even if we have to jump a lot of hoops here.
                if isinstance(item[key], list) and \
                   any(callable(x) for x in _item):
                    from inspect import signature

                    def wrapper(*args, **kwargs):
                        for i, subitem in enumerate(_item):
                            if callable(subitem):
                                subparms = signature(subitem).parameters
                                extra_args = {
                                    k: v
                                    for k, v in kwargs.items()
                                    if k in subparms
                                }
                                _item[i] = subitem(*args, **extra_args)
                        return _item
                    # Gather the arguments
                    parms = tuple(set(flatten([
                        list(signature(x).parameters.values())
                        for x in _item
                        if callable(x)
                    ])))
                    # Rewrite signature
                    wrapper.__signature__ = \
                        signature(wrapper).replace(parameters=parms)
                    item[key] = wrapper
                else:
                    item[key] = _item
        elif isinstance(item, list):
            for i, subitem in enumerate(item):
                item[i] = self.expand(rule, subitem,
                                      expand_args=expand_args, rec=rec)
        elif isinstance(item, tuple):
            item = tuple(self.expand(rule, subitem,
                                     expand_args=expand_args, rec=rec)
                         for subitem in item)
        else:
            raise ValueError("unable to expand item '{}' with args '{}'"
                             "".format(repr(item),
                                       repr(expand_args)))

        if debug:
            log.debug("{}=> {} {}"
                      "".format(" "*(rec*4), type(item).__name__, item))

        return item


class SnakemakeExpander(BaseExpander):
    """Expand wildcards in strings returned from functions.

    Snakemake does not do this by default, leaving wildcard expansion to
    the functions provided themselves. Since we never want `{input}` to be
    in a string returned as a file, we expand those always.
    """
    def expands_field(self, field):
        return field in ('input', 'output')

    def format(self, item, *args, **kwargs):
        if 'wc' in kwargs:
            return apply_wildcards(item, kwargs['wc'])
        return item


class FormatExpander(BaseExpander):
    """
    Expander using a custom formatter object.
    """
    regex = re.compile(
        r"""
        \{
            (?=(
                (?P<name>[^{}]+)
            ))\1
        \}
        """, re.VERBOSE)

    spec = "{{{}}}"

    def __init__(self):
        super().__init__()
        self.formatter = self.Formatter(self)

    def format(self, *args, **kwargs):
        return self.formatter.format(*args, **kwargs)

    class Formatter(ProductFormatter):
        def __init__(self, expander):
            self.expander = expander
            self.spec = expander.spec
            super().__init__()

        def parse(self, format_string):
            if format_string is None:
                return

            start = 0
            for match in self.expander.regex.finditer(format_string):
                yield (format_string[start:match.start()],
                       match.group('name'), '', None)
                start = match.end()

            yield (format_string[start:],
                   None, None, None)

    def get_names(self, pattern):
        return set(match.group('name')
                   for match in self.regex.finditer(pattern))


class ColonExpander(FormatExpander):
    """
    Expander using `{:xyz:}` formatted variables.
    """
    regex = re.compile(
        r"""
        \{:
            (?=(
                \s*
                 (?P<name>(?:.(?!\s*\:\}))*.)
                \s*
            ))\1
        :\}
        """, re.VERBOSE)

    spec = "{{:{}:}}"

    def __init__(self):
        super().__init__()


class RecursiveExpander(BaseExpander):
    """Recursively expands `{xyz}` wildcards in Snakemake rules."""
    def expands_field(self, field):
        """
        Returns true for all fields but `shell:`, `message:` and
        `wildcard_constraints`.

        We don't want to mess with the regular expressions in the fields
        in `wildcard_constraints:`, and there is little use in expanding
        `message` or `shell` as these already have all wildcards applied
        just before job execution (by `format_wildcards()`).
        """
        return field not in (
            'shellcmd',
            'message',
            'wildcard_constraints'
        )

    def expand(self, rule, ruleinfo):
        """Recursively expand wildcards within RuleInfo object"""
        fields = list(filter(None.__ne__,
                             filter(self.expands_field, ruleinfo_fields)))
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
        deps = networkx().DiGraph()
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
                node
                for node in networkx().algorithms.dag.topological_sort(deps)
                if deps.out_degree(node) > 0 and 'core' in deps.nodes[node]
            ]))
        except networkx().NetworkXUnfeasible:
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
                        "Unable to resolve wildcard '{{{}}}' in parameter {}"
                        "in rule {}".format(e.attr, node, rule.name))
                except IndexError as e:
                    raise RuleException(
                        "Unable to format '{}' using '{}'".format(value, args))
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


class InheritanceExpander(BaseExpander):
    """Adds class-like inheritance to Snakemake rules

    To avoid redundancy between closely related rules, e.g. rules for
    single ended and paired end data, YMP allows Snakemake rules
    to inherit from another rule.

    Example:
     .. code-block: snakemake
      rule count_reads:
        input: "{file}.R1.fq.gz", "{file}.R2.fq.gz"
        output: "{file}.readcount.txt"
        shell: "gunzip -c {input} | wc -l > {output}"

      rule count_reads_SE:  # ymp: extends count_reads
        input: "{file}.fq.gz"

    Derived rules are always created with an implicit ``ruleorder`` statement,
    making Snakemake prefer the parent rule if either parent or child rule
    could be used to generate the requested output file(s).

    Derived rules initially contain the same attributes as the parent rule.
    Each attribute assigned to the child rule overrides the matching attribute
    in the parent. Where attributes may contain named and unnamed values,
    specifying a named value overrides only the value of that name while
    specifying an unnamed value overrides all unnamed values in the parent
    attribute.
    """
    # FIXME: link to http://snakemake.readthedocs.io/en/latest/snakefiles/
    #                rules.html#handling-ambiguous-rules

    #: Comment keyword enabling inheritance
    KEYWORD = "ymp: extends"

    def __init__(self):
        super().__init__()
        self.ruleinfos = {}
        self.snakefiles = {}
        self.linemaps = None
        log.debug("Ineritance Enabled")

    def get_code_line(self, rule: Rule) -> str:
        """Returns the source line defining *rule*"""
        # Load and cache Snakefile
        if rule.snakefile not in self.snakefiles:
            try:
                with open(rule.snakefile, "r") as sf:
                    self.snakefiles[rule.snakefile] = sf.readlines()
            except IOError:
                raise Exception("Can't parse ...")

        # `rule.lineno` refers to compiled code. Convert to source line number.
        if self.linemaps is None:
            self.linemaps = ExpandableWorkflow.global_workflow.linemaps
        real_lineno = self.linemaps[rule.snakefile][rule.lineno]

        return self.snakefiles[rule.snakefile][real_lineno - 1]

    def get_super(self, rule: Rule, ruleinfo: RuleInfo) -> Optional[RuleInfo]:
        """Find rule parent

        Args:
          rule: Rule object being built
          ruleinfo: RuleInfo object describing rule being built

        Returns:
          2-Tuple: name of parent rule and RuleInfo describing parent rule
          or (None, None).
        """
        self.ruleinfos[rule.name] = ruleinfo  # stash original ruleinfos

        if hasattr(ruleinfo, 'parent'):
            return ruleinfo.parent.name, self.ruleinfos[ruleinfo.parent.name]

        line = self.get_code_line(rule)

        if "#" in line:
            comment = line.split("#")[1].strip()
            if comment.startswith(self.KEYWORD):
                superrule_name = comment[len(self.KEYWORD):].strip()
                try:
                    return superrule_name, self.ruleinfos[superrule_name]
                except:
                    raise InheritanceException("Unable to find parent",
                                               rule, superrule_name)
        return None, None

    def expand(self, rule, ruleinfo):
        super_name, super_ruleinfo = self.get_super(rule, ruleinfo)
        if super_ruleinfo is None:
            return

        base_ruleinfo = deepcopy(super_ruleinfo)

        if not ruleinfo.norun:  # deriving rule is runnable, clear out base
            base_ruleinfo.shellcmd = None
            base_ruleinfo.wrapper = None
            base_ruleinfo.script = None
            base_ruleinfo.func = None
        elif not base_ruleinfo.norun:  # base is runnable, clear our deriving
            ruleinfo.norun = False
            ruleinfo.shellcmd = None
            ruleinfo.wrapper = None
            ruleinfo.script = None
            ruleinfo.func = None

        for field in dir(ruleinfo):
            if field.startswith("__") or field == "parent":
                continue

            base_attr = getattr(base_ruleinfo, field)
            override_attr = getattr(ruleinfo, field)

            if isinstance(override_attr, tuple):
                if base_attr is None:
                    base_attr = ([], {})
                if override_attr[0]:
                    base_attr = (override_attr[0], base_attr[1])
                if override_attr[1]:
                    base_attr[1].update(override_attr[1])
            elif override_attr is not None:
                base_attr = override_attr

            setattr(ruleinfo, field, base_attr)

        if not super_ruleinfo.norun:
            if super_name in self.ruleinfos:
                self.workflow.ruleorder(super_name, rule.name)


class DefaultExpander(InheritanceExpander):
    """
    Adds default values to rules

    The implementation simply makes all rules inherit from a defaults
    rule.
    """
    def __init__(self, **kwargs):
        """
        Creates DefaultExpander

        Each parameter passed is considered a RuleInfo default value. Where
        applicable, Snakemake's argtuples `([],{})` must be passed.
        """
        super().__init__()
        self.defaults = RuleInfo(None)
        self.defaults.norun = True

        for key, value in kwargs.items():
            setattr(self.defaults, key, value)

    def get_super(self, rule: Rule, ruleinfo: RuleInfo) -> RuleInfo:
        return ("__default__", self.defaults)
