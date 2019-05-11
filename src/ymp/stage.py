"""
YMP processes data in stages, each of which is contained in its own directory.

.. code-block:: snakemake

  with Stage("trim_bbmap") as S:
    S.doc("Trim reads with BBMap")
    rule bbmap_trim:
      output: "{:this:}/{sample}{:pairnames:}.fq.gz"
      input:  "{:prev:}/{sample}{:pairnames:}.fq.gz"
      ...

"""

import logging
import re
from typing import TYPE_CHECKING

import ymp
from ymp.exceptions import YmpException, YmpRuleError, YmpStageError
from ymp.snakemake import ColonExpander, ExpandLateException, WorkflowObject
from ymp.string import PartialFormatter


if TYPE_CHECKING:
    from typing import List
    from snakemake.rules import Rule

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


def norm_wildcards(pattern):
    for n in ("{target}", "{source}", "{:target:}"):
        pattern = pattern.replace(n, "{sample}")
    for n in ("{:targets:}", "{:sources:}"):
        pattern = pattern.replace(n, "{:samples:}")
    return pattern


class StageStack(object):
    """The "head" of a processing chain - a stack of stages
    """

    @classmethod
    def get(cls, path, stage=None):
        """
        Cached access to StageStack

        Args:
          path: Stage path
          stage: Stage object at head of stack
        """
        cfg = ymp.get_config()
        cache = cfg.cache.get_cache(cls.__name__, itemloadfunc=StageStack)
        return cache[path]

    def __str__(self):
        return self.path

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name}, {self.stage})"

    def __init__(self, path, stage=None):
        self.name = path
        # immediate predecessors (sources) for the Stage of this Stack
        self.prevs = []

        cfg = ymp.get_config()
        registry = Stage.get_registry()
        stage_names = path.split(".")

        # project for this stage stack
        self.project = cfg.projects.get(stage_names[0])
        if not self.project:
            raise YmpStageError(f"No project for stage stack {path} found")

        top_stage = stage_names.pop()
        if stage:
            if not stage.match(top_stage):
                raise YmpStageError(
                    f"Internal error: {top_stage} not matched by {stage}")
        if not stage:
            stage = self._find_stage(top_stage)
        self.stage = stage

        self.path = getattr(stage, "dir", path)

        self.group = getattr(stage, "group", None)
        if stage_names and stage_names[-1].startswith("group_"):
            self.group = [stage_names.pop().split("_")[1]]

        # gather prev stage stacks for this stack, going backwards
        # through stack until all inputs have been satisfied
        inputs = set(self.stage.inputs)
        while stage_names and inputs:
            path = ".".join(stage_names)
            stage = self._find_stage(stage_names.pop())
            if not stage:
                continue
            stack = self.get(path, stage)
            provides = inputs.intersection(stack.outputs)
            if provides:
                inputs -= provides
                self.prevs.append(stack)

        if inputs:
            # Can't find the right types, try to present useful error message:
            words = []
            for suffix in inputs:
                words.extend((suffix, "--"))
                words.extend([name for name, stage in registry.items()
                              if suffix in stage.outputs])
                words.extend('\n')
            text = ' '.join(words)

            raise YmpStageError(
                f"""
                File type(s) '{" ".join(inputs)}' required by '{self.stage}'
                not found in '{self.name}'. Stages providing missing
                file types:
                {text}
                """
            )

        if self.group is None:
            groups = list(dict.fromkeys(group
                                        for p in reversed(self.prevs)
                                        for group in p.group))
            self.group = self.project.minimize_variables(groups)

        log.info("Stage stack %s using column %s", self, self.group)

    def _find_stage(self, name):
        cfg = ymp.get_config()
        registry = Stage.get_registry()

        if name.startswith("group_"):
            return None  ## fails FIXME
        if name.startswith("ref_"):
            refname = name[4:]
            if refname in cfg.ref:
                return cfg.ref[refname]
            else:
                raise YmpStageError(f"Unknown reference '{cfg.ref[refname]}'")
        for stage in registry.values():
            if stage.match(name):
                return stage
        raise YmpStageError(f"Unknown stage '{name}'")

    def complete(self, incomplete):
        registry = Stage.get_registry()
        cfg = ymp.get_config()
        result = []
        groups = ("group_" + name for name in self.project.variables)
        result += (opt for opt in groups if opt.startswith(incomplete))
        refs = ("ref_" + name for name in cfg.ref)
        result += (opt for opt in refs if opt.startswith(incomplete))
        for stage in registry.values():
            for name in (stage.name, stage.altname):
                if name and name.startswith(incomplete):
                    try:
                        self.get(".".join((self.path, name)))
                        result.append(name)
                    except YmpStageError:
                        pass

        return result

    @property
    def outputs(self):
        return self.stage.outputs

    @property
    def defined_in(self):
        return None

    def prev(self, args=None, kwargs=None):
        """
        Directory of previous stage
        """
        _, _, suffix = kwargs.get('item').partition("{:prev:}")
        if not kwargs or "wc" not in kwargs:
            raise ExpandLateException()
        suffix = norm_wildcards(suffix)

        for stack in self.prevs:
            if suffix in stack.outputs:
                return stack

    @property
    def targets(self):
        """
        Returns the current targets
        """
        return self.project.get_ids(self.group)

    def target(self, args, kwargs):
        """Finds the target in the prev stage matching current target"""
        prev = self.get(self.prev(args, kwargs).name)
        cur_target = kwargs['wc'].target
        return self.project.get_ids(prev.group, self.group, cur_target)


class Stage(WorkflowObject):
    """
    Creates a new stage

    While entered using ``with``, several stage specific variables
    are expanded within rules:

    * ``{:this:}`` -- The current stage directory
    * ``{:that:}`` -- The alternate output stage directory
    * ``{:prev:}`` -- The previous stage's directory

    """

    active = None
    """Currently active stage ("entered")"""

    def __init__(self, name: str, altname: str=None,
                 env: str=None, doc: str=None) -> None:
        """
        Args:
            name: Name of this stage
            altname: Alternate name of this stage (used for stages with
                multiple output variants, e.g. filter_x and remove_x.
            doc: See `Stage.doc`
            env: See `Stage.env`
        """
        # Stage name
        self.name: str = name
        # Alternate stage name
        self.altname: str = altname
        super().__init__()
        self.register()
        # Rules in this stage
        self.rules: List[Rule] = []
        # Stage Parameters
        self.params: List[Param] = []
        # Input / Output types
        self.inputs = set()
        self.outputs = set()
        # Regex matching self
        self._regex = None

        self.doc(doc or "")
        self.env(env)

    def doc(self, doc: str) -> None:
        """Add documentation to Stage

        Args:
          doc: Docstring passed to Sphinx
        """
        self.docstring = doc

    def env(self, name: str) -> None:
        """Add package specifications to Stage environment

        Note:
          This sets the environment for all rules within the stage,
          which leads to errors with Snakemake rule types
          not supporting conda environments

        Args:
          name: Environment name or filename

        >>> Env("blast", packages="blast =2.7*")
        >>> with Stage("test") as S:
        >>>    S.env("blast")
        >>>    rule testing:
        >>>       ...

        >>> with Stage("test", env="blast") as S:
        >>>    rule testing:
        >>>       ...

        >>> with Stage("test") as S:
        >>>    rule testing:
        >>>       conda: "blast"
        >>>       ...
        """
        self.conda_env = name

    def __enter__(self):
        if Stage.active is not None:
            raise YmpRuleError(
                self,
                f"Failed to enter stage '{self.name}', "
                f"already in stage {self.active.name}'."
            )

        Stage.active = self
        return self

    def __exit__(self, *args):
        Stage.active = None

    def __str__(self):
        if self.altname:
            return "|".join((self.name, self.altname))
        return self.name

    def __repr__(self):
        return (f"{self.__class__.__name__} {self!s} "
                f"({self.filename}:{self.lineno})")

    def _add_rule(self, rule):
        rule.ymp_stage = self
        # FIXME: disabled because it breaks pickling of Stage
        # self.rules.append(rule)

    def add_param(self, key, typ, name, value=None, default=None):
        """Add parameter to stage

        Example:
            >>> with Stage("test") as S
            >>>   S.add_param("N", "int", "nval", default=50)
            >>>   rule:
            >>>      shell: "echo {param.nval}"

            This would add a stage "test", optionally callable as "testN123",
            printing "50" or in the case of "testN123" printing "123".

        Args:
          char: The character to use in the Stage name
          typ:  The type of the parameter (int, flag)
          param: Name of parameter in params
          value: value ``{param.xyz}`` should be set to if param given
          default: default value for ``{{param.xyz}}`` if no param given
        """
        if typ == 'flag':
            self.params.append(ParamFlag(self, key, name, value, default))
        elif typ == 'int':
            self.params.append(ParamInt(self, key, name, value, default))
        elif typ == 'choice':
            self.params.append(ParamChoice(self, key, name, value, default))
        else:
            raise YmpRuleError(self, f"Unknown Stage Parameter type '{typ}'")

    def wc2path(self, wc):
        wildcards = self.wildcards
        for p in self.params:
            wildcards = wildcards.replace(p.constraint, "")
        return wildcards.format(**wc)

    def stack(self, wc):
        return StageStack.get(self.wc2path(wc), self)

    def match(self, name):
        if not self._regex:
            if self.altname:
                sname = "(" + "|".join((self.name, self.altname)) + ")"
            else:
                sname = self.name
            pat = sname + "".join(p.regex for p in self.params)
            self._regex = re.compile(pat)
        return self._regex.fullmatch(name) is not None

    def prev(self, args, kwargs):
        """Gathers {:prev:} calls from rules"""
        prefix, _, suffix = kwargs.get('item').partition("{:prev:}")
        if not prefix:
            self.inputs.add(norm_wildcards(suffix))
        return None

    def this(self, args=None, kwargs=None):
        """
        Directory of current stage
        """
        if not Stage.active:
            raise YmpException(
                "Use of {:this:} requires active Stage"
            )
        prefix, _, suffix = kwargs.get('item').partition("{:this:}")
        if not prefix:
            self.outputs.add(norm_wildcards(suffix))

        item = kwargs.get('item')
        if item is None:
            raise YmpException("Internal Error")
        prefix, _, pattern = item.partition("{:this:}")

        return self.wildcards

    @property
    def that(self):
        """
        Alternate directory of current stage

        Used for splitting stages
        """
        if not Stage.active:
            raise YmpException(
                "Use of {:that:} requires active Stage"
            )
        if not Stage.active.altname:
            raise YmpException(
                "Use of {:that:} requires with altname"
            )
        return "".join(["{_YMP_DIR}", self.altname] +
                       [p.pattern for p in self.params])

    @property
    def wildcards(self):
        return "".join(["{_YMP_DIR}", self.name] +
                       [p.pattern for p in self.params])


class StageExpander(ColonExpander):
    """
    Registers rules with stages when they are created
    """
    def expand_ruleinfo(self, rule, item, expand_args, rec):
        if not Stage.active:
            return item
        stage = Stage.active

        stage._add_rule(rule)
        if not item.conda_env and stage.conda_env:
            item.conda_env = stage.conda_env

        if stage.params:
            if not item.params:
                item.params = ((), {})
            for param in stage.params:
                item.params[1][param.name] = param.param_func()

        return super().expand_ruleinfo(rule, item, expand_args, rec)

    def expand_str(self, rule, item, expand_args, rec, cb):
        if cb:
            stage = Stage.active
            Stage.active = rule.ymp_stage
        expand_args['item'] = item
        val = super().expand_str(rule, item, expand_args, rec, cb)
        if cb:
            Stage.active = stage
        return val

    def expands_field(self, field):
        return field not in 'func'

    class Formatter(ColonExpander.Formatter, PartialFormatter):
        def get_value(self, key, args, kwargs):
            try:
                return self.get_value_(key, args, kwargs)
            except Exception as e:
                if not isinstance(e, ExpandLateException):
                    log.warning(f"key={key} args={args} kwargs={kwargs}",
                                exc_info=True)
                raise

        def get_value_(self, key, args, kwargs):
            stage = Stage.active
            if hasattr(stage, key):
                val = getattr(stage, key)
                if hasattr(val, "__call__"):
                    val = val(args, kwargs)
                if val is not None:
                    return val
            if "wc" not in kwargs:
                raise ExpandLateException()
            stack = stage.stack(kwargs['wc'])
            if hasattr(stack, key):
                val = getattr(stack, key)
                if hasattr(val, "__call__"):
                    val = val(args, kwargs)
                if val is not None:
                    return val
            if hasattr(stack.project, key):
                val = getattr(stack.project, key)
                if hasattr(val, "__call__"):
                    val = val(args, kwargs)
                if val is not None:
                    return val
            return super().get_value(key, args, kwargs)


class Param(object):
    """Stage Parameter (base class)"""
    def __init__(self, stage, key, name, value=None, default=None):
        self.stage = stage
        self.key = key
        self.name = name
        self.value = value
        self.default = default

        self.wildcard = f"_yp_{self.name}"

    @property
    def constraint(self):
        if self.regex:
            return "," + self.regex
        return ""

    @property
    def pattern(self):
        """String to add to filenames passed to Snakemake

        I.e. a pattern of the form ``{wildcard,constraint}``
        """
        return f"{{{self.wildcard}{self.constraint}}}"


class ParamFlag(Param):
    """Stage Flag Parameter"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.value:
            self.value = self.key

        self.regex = f"({self.key}?)"

    def param_func(self):
        """Returns function that will extract parameter value from wildcards"""
        def name2param(wildcards):
            if getattr(wildcards, self.wildcard, None):
                return self.value
            else:
                return ""
        return name2param


class ParamInt(Param):
    """Stage Int Parameter"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.default:
            raise YmpRuleError(
                self.stage,
                f"Stage Int Parameter must have 'default' set")

        self.regex = f"({self.key}\d+|)"

    def param_func(self):
        """Returns function that will extract parameter value from wildcards"""
        def name2param(wildcards):
            val = getattr(wildcards, self.wildcard, None)
            if val:
                return val[len(self.key):]
            else:
                return self.default
        return name2param


class ParamChoice(Param):
    """Stage Choice Parameter"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.default is not None:
            self.value += [""]
        self.regex = f"({self.key}({'|'.join(self.value)}))"

    def param_func(self):
        """Returns function that will extract parameter value from wildcards"""
        def name2param(wildcards):
            val = getattr(wildcards, self.wildcard, None)
            if val:
                return val[len(self.key):]
            else:
                return self.default
        return name2param
