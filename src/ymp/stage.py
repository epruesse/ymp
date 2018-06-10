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
from typing import TYPE_CHECKING

from ymp.exceptions import YmpRuleError, YmpException
from ymp.snakemake import ColonExpander, RuleInfo, WorkflowObject
from ymp.string import PartialFormatter

if TYPE_CHECKING:
    from typing import List
    from snakemake.rules import Rule

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


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

        Args:
          name: Environment name or filename

        Example:
          Env("bowtie2", packages="blast =2.7*")
          with Stage("test") as S:
            S.env("bowtie2")
        """
        self.conda_env = name

    def __enter__(self):
        if Stage.active is not None:
            raise YmpRuleError(
                self,
                "Failed to enter stage '{self.name}', "
                "already in stage {self.active.name}'."
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
        return f"{self.__class__.__name__} ({self.filename}:{self.lineno})"

    def _add_rule(self, rule):
        rule.ymp_stage = self
        self.rules.append(rule)

    def add_param(self, char, typ, param, value=None, default=None):
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
          param: The name under which the parameter value should appear in params
          value: [for flag] value ``{param.xyz}`` should be set to if param given
          default [for int] default value for `{{param.xyz}}`` if no param given
        """
        if typ == 'flag':
            self.params.append(ParamFlag(self, char, param, value, default))
        elif typ == 'int':
            self.params.append(ParamInt(self, char, param, value, default))
        else:
            raise YmpRuleError(self, f"Unknown Stage Parameter type '{typ}'")

    @property
    def prev(self):
        """
        Directory of previous stage
        """
        return "{_YMP_PRJ}{_YMP_DIR}"

    @property
    def this(self):
        """
        Directory of current stage
        """
        if not Stage.active:
            raise YmpException(
                "Use of {:this:} requires active Stage"
            )
        stage = Stage.active

        return "".join([
            self.prev,
            "{_YMP_VRT}{_YMP_ASM}.",
            stage.name,
            "".join(p.pattern for p in stage.params)
        ])

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
        return "".join([
            self.prev,
            "{_YMP_VRT}{_YMP_ASM}.",
            Stage.active.altname
        ])


class StageExpander(ColonExpander):
    """
    Registers rules with stages when they are created
    """
    def expand(self, rule, item, **kwargs):
        if not Stage.active:
            return item
        stage = Stage.active

        if isinstance(item, RuleInfo):
            stage._add_rule(rule)
            if not item.conda_env and stage.conda_env:
                item.conda_env = stage.conda_env

            if stage.params:
                if not item.params:
                    item.params = ((), {})
                for param in stage.params:
                    item.params[1][param.param] = param.param_func()

        # run colon expand
        return super().expand(rule, item, **kwargs)

    def expands_field(self, field):
        return field not in 'func'

    class Formatter(ColonExpander.Formatter, PartialFormatter):
        def get_value(self, key, args, kwargs):
            stage = Stage.active
            if hasattr(stage, key):
                return getattr(stage, key)
            return super().get_value(key, args, kwargs)


class Param(object):
    """Stage Parameter (base class)"""
    def __init__(self, stage, char, param, value=None, default=None):
        if len(char) != 1:
            raise YmpRuleError(
                stage,
                f"Stage Parameter key '{char}' invalid. Must be length 1.")
        self.stage = stage
        self.char = char
        self.param = param
        self.value = value
        self.default = default

        self.wildcard = f"_yp_{self.char}"
        self.constraint = ""

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
            raise YmpRuleError(
                self.stage,
                f"Stage Flag Parameter must have 'value' set")

        self.constraint = f",({self.char}?)"

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

        self.constraint = f",({self.char}\d+|)"

    def param_func(self):
        """Returns function that will extract parameter value from wildcards"""
        def name2param(wildcards):
            val = getattr(wildcards, self.wildcard, None)
            if val:
                return val[1:]
            else:
                return self.default
        return name2param
