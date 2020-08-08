import copy
import logging
import re

from ymp.snakemake import WorkflowObject
from ymp.stage.base import BaseStage

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


def norm_wildcards(pattern):
    for n in ("{target}", "{source}", "{:target:}"):
        pattern = pattern.replace(n, "{sample}")
    for n in ("{:targets:}", "{:sources:}"):
        pattern = pattern.replace(n, "{:samples:}")
    return pattern


class Stage(WorkflowObject, BaseStage):
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
        super().__init__(name)
        # Alternate stage name
        self.altname: str = altname
        self.register()
        # Rules in this stage
        self.rules: List[Rule] = []
        # Inputs required by stage
        self._inputs: Set[str] = set()
        # Stage Parameters
        self.params: List[Param] = []
        self.requires = None
        # Regex matching self
        self._regex = None

        self.doc(doc or "")
        self.env(env)

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

    def get_path(self, suffix=None):
        return None

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

    def require(self, **kwargs):
        """Override inferred stage inputs

        In theory, this should not be needed. But it's simpler for now.
        """
        self.requires = kwargs

    def get_inputs(self):
        if not self.requires:
            return copy.copy(self._inputs)
        return copy.copy(self.requires)

    def satisfy_inputs(self, other_stage, inputs):
        if not self.requires: # inputs is set
            provides = other_stage.can_provide(inputs)
            inputs -= provides
            return provides

        provides = set()
        keys = set()
        for key, input_alts in inputs.items():
            for input_alt in input_alts:
                have = other_stage.can_provide(set(
                    "/{{sample}}.{}".format(ext) for ext in input_alt
                ))
                if len(have) == len(input_alt):
                    provides.update(have)
                    keys.add(key)
                    break
        for key in keys:
            del inputs[key]
        return provides

    def wc2path(self, wc):
        wildcards = self.wildcards()
        for p in self.params:
            wildcards = wildcards.replace(p.constraint, "")
        return wildcards.format(**wc)

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
            self._inputs.add(norm_wildcards(suffix))
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

        return self.wildcards(args=args, kwargs=kwargs)

    def that(self, args=None, kwargs=None):
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
        show_constraint = kwargs and kwargs.get('field') != 'input'
        return "".join(["{_YMP_DIR}", self.altname] +
                       [p.pattern(show_constraint) for p in self.params])

    def wildcards(self, args=None, kwargs=None):
        show_constraint = kwargs and kwargs.get('field') != 'input'
        return "".join(["{_YMP_DIR}", self.name] +
                       [p.pattern(show_constraint) for p in self.params])




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

    def pattern(self, show_constraint=True):
        """String to add to filenames passed to Snakemake

        I.e. a pattern of the form ``{wildcard,constraint}``
        """
        if show_constraint:
            return f"{{{self.wildcard}{self.constraint}}}"
        else:
            return f"{{{self.wildcard}}}"


class ParamFlag(Param):
    """Stage Flag Parameter"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.value:
            self.value = self.key

        self.regex = f"((?:{self.key})?)"

    def param_func(self):
        """Returns function that will extract parameter value from wildcards"""
        def name2param(wildcards):
            if getattr(wildcards, self.wildcard, None):
                return self.value
            else:
                return self.default or ""
        return name2param


class ParamInt(Param):
    """Stage Int Parameter"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.default is None:
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
