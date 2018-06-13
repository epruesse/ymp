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
from ymp.exceptions import YmpRuleError, YmpException
from ymp.snakemake import ColonExpander, RuleInfo, WorkflowObject, ExpandLateException
from ymp.string import PartialFormatter

if TYPE_CHECKING:
    from typing import List
    from snakemake.rules import Rule

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class StageStack(object):
    """
    Computes available targets from stage stack encoded in directory name

    sources:
    targets:
    target:
    reference:

    Computes the current groups and group members based on
    the 'context': DatasetConfig and wildcards
    """
    RE_BY = re.compile(r"\.by_([^./]*)(?:[./]|$)")

    def __init__(self, path):
        self.stages = path.split(".")
        self.group = None
        for stage in reversed(self.stages):
            if stage.startswith("by_"):
                self.group = stage[3:]
                break
        if self.group is None:
            self.group = "ALL"
        cfg = ymp.get_config()
        self.project = cfg.projects[self.stages[0]]

    @property
    def group_by(self):
        df = self.project.run_data

        if self.group == "ALL":
            import pandas as pd
            return df.groupby(pd.Series("ALL", index=df.index))
        elif self.group == "ID":
            return df.groupby(df.index)
        else:
            try:
               return df.groupby(self.group)
            except KeyError:
                raise YmpConfigError("Unkown column in groupby: {self.group}")

    @property
    def reference(self):
        """
        Returns the currently selected reference
        """
        references = self.dcfg.cfgmgr.ref.keys()
        re_ref = re.compile(r"\.(ref_(?:{})|assemble_(?:megahit|metaspades|trinity))(?=[./]|$)"
                            r"".format("|".join(references)))
        stackstr = "".join(
            getattr(self.wc, key)
            for key in ['dir', '_YMP_PRJ', '_YMP_DIR', '_YMP_VRT', '_YMP_ASM']
            if hasattr(self.wc, key)
        )
        matches = re_ref.findall(stackstr)

        if not matches:
            raise KeyError("No reference found for {} and {!r}"
                           "".format(self.rule, self.wc))

        ref_name = matches[-1]
        if ref_name.startswith("ref_"):
            reference = self.dcfg.cfgmgr.ref[ref_name[4:]]
        else:
            target = getattr(self.wc, 'target', 'ALL')
            reference = "{}/{}.contigs".format(stackstr, target)

        log.debug("Reference selected for {}: {}".format(self.rule, reference))

        return reference

    @property
    def targets(self):
        """
        Returns the current targets:

         - all "runs" if no by_COLUMN is active
         - the unique values for COLUMN if grouping is active
        """
        return list(self.group_by.indices)

    def sources(self, wc):
        """
        Returns the runs associated with the current target
        """
        target = wc.get("target")
        if not target:
            raise YmpException("Using '{:sources:}' requires '{target}' wildcard")

        try:
            sources = self.group_by.groups[target]
        except KeyError:
            log.debug(list(wc.allitems()))
            raise YmpException(
                "Target '{}' not available. Possible choices are '{}'"
                "".format(target, list(self.group_by.groups.keys()))
            )
        return sources


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
        elif typ == 'choice':
            self.params.append(ParamChoice(self, char, param, value, default))
        else:
            raise YmpRuleError(self, f"Unknown Stage Parameter type '{typ}'")

    def prev(self, args=None, kwargs={}):
        """
        Directory of previous stage
        """
        if "wc" not in kwargs:
            raise ExpandLateException()
        item = kwargs['item']
        _, _, item = item.partition("{:prev:}")

        if self.name == "assemble_megahit":
            log.debug("prev: %s", item)

        return "{_YMP_PRJ}{_YMP_DIR}"

    def sources(self, args=None, kwargs=None):
        if not kwargs or "wc" not in kwargs:
           raise ExpandLateException()
        wc = kwargs["wc"]
        path = self.get_path(wc)
        stack = StageStack(path)
        return stack.sources(wc)

    def targets(self, args=None, kwargs=None):
        if not kwargs or "wc" not in kwargs:
           raise ExpandLateException()
        wc = kwargs["wc"]
        path = self.get_path(wc)
        stack = StageStack(path)
        return stack.targets

    def get_path(self, wc):
        return "".join(val for key, val in wc.allitems() if key.startswith("_YMP"))

    def this(self, args=None, kwargs={}):
        """
        Directory of current stage
        """
        if not Stage.active:
            raise YmpException(
                "Use of {:this:} requires active Stage"
            )

        item = kwargs.get('item')
        if item is None:
            raise YmpException("Internal Error")
        prefix, _, pattern = item.partition("{:this:}")

        #if pattern:
        #    log.error("Stage %s: %s", self.name, pattern)

        return "".join([
            "{_YMP_PRJ}{_YMP_DIR}",
            "{_YMP_VRT}{_YMP_ASM}.",
            self.name,
            "".join(p.pattern for p in self.params)
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
            "{_YMP_PRJ}{_YMP_DIR}",
            "{_YMP_VRT}{_YMP_ASM}.",
            Stage.active.altname
        ])


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
                item.params[1][param.param] = param.param_func()

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
            stage = Stage.active
            if hasattr(stage, key):
                val = getattr(stage, key)
                if isinstance(val, str):
                    return val
                return val(args, kwargs)

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


class ParamChoice(Param):
    """Stage Choice Parameter"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.default or not self.value:
            raise YmpRuleError(
                self.stage,
                f"Stage Choice Parameter must have 'default' and 'value' set")
        self.constraint = f",({self.char}({'|'.join(self.value)})|)"

    def param_func(self):
        """Returns function that will extract parameter value from wildcards"""
        def name2param(wildcards):
            val = getattr(wildcards, self.wildcard, None)
            if val:
                return val[1:]
            else:
                return self.default
        return name2param
