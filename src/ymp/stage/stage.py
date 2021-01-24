"""
Implements the "Stage"

At it's most basic, a "Stage" is a set of Snakemake rules that share an output folder.
"""

import copy
import logging
import re

from typing import Dict, List, Set

from ymp.snakemake import WorkflowObject, RemoveValue
from ymp.stage.base import BaseStage, Activateable
from ymp.exceptions import YmpRuleError, YmpException, YmpStageError

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class Stage(WorkflowObject, Activateable, BaseStage):
    """
    Creates a new stage

    While entered using ``with``, several stage specific variables
    are expanded within rules:

    * ``{:this:}`` -- The current stage directory
    * ``{:that:}`` -- The alternate output stage directory
    * ``{:prev:}`` -- The previous stage's directory
    """

    def __init__(
            self,
            name: str,
            altname: str = None,
            env: str = None,
            doc: str = None,
    ) -> None:
        """
        Args:
            name: Name of this stage
            altname: Alternate name of this stage (used for stages with
                multiple output variants, e.g. filter_x and remove_x.
            doc: See `Stage.doc`
            env: See `Stage.env`
        """
        super().__init__(name)
        #: Alternative stage name (deprecated)
        self.altname: str = altname
        #: Checkpoints in this stage
        self.checkpoints: Dict[str, Set[str]] = {}
        #: Contains override stage inputs
        self.requires = None
        #: Stage Parameters
        self.params: List[Param] = []

        # Inputs required by stage
        self._inputs: Set[str] = set()
        self._outputs: Set[str] = set()
        # Regex matching self
        self._regex = None

        self.register()

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

    def __str__(self):
        if self.altname:
            return "|".join((self.name, self.altname))
        return self.name

    def __repr__(self):
        return (f"{self.__class__.__name__} {self!s} "
                f"({self.filename}:{self.lineno})")

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

    @property
    def outputs(self) -> Set[str]:
        return self._outputs

    def get_inputs(self):
        if not self.requires:
            return copy.copy(self._inputs)
        return copy.copy(self.requires)

    def satisfy_inputs(self, other_stage, inputs) -> Dict[str, str]:
        if not self.requires:  # inputs is set
            provides = other_stage.can_provide(inputs)
            # warning: `inputs -= provides.keys()` would work, but would
            # create a new object, rather than modify the set we
            # were passed.
            inputs -= set(list(provides.keys()))
            return provides

        provides = dict()
        keys = set()
        for key, input_alts in inputs.items():
            for input_alt in input_alts:
                have = other_stage.can_provide(set(
                    "/{{sample}}.{}".format(ext) for ext in input_alt
                ))
                if len(have) == len(input_alt):
                    have_new = {output: path
                                for output, path in have.items()
                                if output not in provides}
                    provides.update(have_new)
                    keys.add(key)
                    break
        for key in keys:
            del inputs[key]
        return provides

    def wc2path(self, wc):
        wildcards = self._wildcards(self.name)
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

    def _wildcards(self, name, kwargs=None):
        show_constraint = kwargs and kwargs.get('field') != 'input'
        return "".join(["{_YMP_DIR}", name] +
                       [p.pattern(show_constraint) for p in self.params])

    def prev(self, _args, kwargs) -> None:
        """Gathers {:prev:} calls from rules

        Here, input requirements for each stage are collected.
        """
        item = kwargs['item']
        self.register_inout("prev", self._inputs, item)

    def this(self, _args=None, kwargs=None):
        """Replaces {:this:} in rules

        Also gathers output capabilities of each stage.
        """
        item = kwargs['item']
        # Fixme: check that this is from outputs
        self.register_inout("this", self._outputs, item)
        return self._wildcards(self.name, kwargs=kwargs)

    def that(self, _args=None, kwargs=None):
        """
        Alternate directory of current stage

        Used for splitting stages
        """
        self.check_active_stage("that")
        if not self.altname:
            raise YmpException(
                "Use of {:that:} requires with altname"
            )
        return self._wildcards(self.altname, kwargs=kwargs)

    def bin(self, _args=None, kwargs=None):
        """
        Dynamic ID for splitting stages
        """
        rule = kwargs['rule']
        if not rule.is_checkpoint:
            raise YmpStageError("Only checkpoints may use '{:bin:}'")
        item = kwargs['item']
        norm_item = item.replace(".{:bin:}", "")
        norm_suffix = self.register_inout("this", self._outputs, norm_item)
        self.checkpoints.setdefault(rule.name, set()).add(norm_suffix)
        raise RemoveValue()

    def has_checkpoint(self) -> bool:
        return bool(self.checkpoints)

    def get_group(
            self,
            stack,  #: "StageStack"
            default_groups: List[str]
    ) -> List[str]:
        # Are we instructed by previous stack to change grouping?
        override_groups = None
        if stack.prev_stack is not None:
            prev_stage = stack.prev_stack.stage
            override_groups = prev_stage.modify_next_group(stack.prev_stack)

        if override_groups is None:
            # If not, just use the default groups
            groups = default_groups
        else:
            # Otherwise, use the override groups,
            groups = override_groups
            # Replace "__bin__" with bins in effect
            if "__bin__" in override_groups:
                groups = [g for g in groups if g != "__bin__"]
                # FIXME:
                # Should we just use the latest bin? What if we have multiple?
                groups += [
                    g for g in default_groups if isinstance(g, type(stack))
                ]

        # If we are a checkpoint ourselves, add self.
        if self.has_checkpoint():
            groups.append(stack)

        return groups

    def get_checkpoint_ids(self, stack, mygroup, target):
        from snakemake.workflow import checkpoints
        if len(self.checkpoints) > 1:
            raise RuntimeError("Multiple checkpoints not implemented")
        checkpoint_name = next(iter(self.checkpoints.keys()))
        checkpoint = getattr(checkpoints, checkpoint_name)
        ymp_dir = stack.path[:-len(stack.stage.name)]
        mytarget = self.get_ids(stack,
                                [g for g in stack.group if g != stack],
                                mygroup, target)

        job = checkpoint.get(_YMP_DIR=ymp_dir, target="__".join(mytarget))
        with open(job.output.bins, "r") as fd:
            bins = [line.strip() for line in fd.readlines()]
        return bins

    def get_ids(self, stack, groups, mygroups=None, target=None):
        # Make a copy of the input gorups, we don't want to modify it.
        groups = groups.copy()
        if mygroups is not None:
            mygroups = list(mygroups)

        bins = []
        mybins = {}
        if mygroups is None and target is None:
            # If we are getting IDs for {:targets:} of binning stage,
            # don't return self
            if stack in groups:
                groups.remove(stack)
            # If we are getting IDs for {:targets:} of subsequent stage,
            # find all generated ids from binning stage.
            # Multiply binned ids
            for group in list(groups):
                if not isinstance(group, type(stack)):
                    continue
                groups.remove(group)
                bins.append(group)
        elif mygroups is None:
            raise RuntimeError("Mygroups none but target not?")
        else:
            # If we are getting IDs for {:target:} of subsequent stage,
            # find all generated ids from binning stage.
            # Multiply binned ids
            for group in list(groups):
                if not isinstance(group, type(stack)):
                    continue
                groups.remove(group)
                bins.append(group)

            target_parts = []
            for group, tgt in zip(list(mygroups), target.split("__")):
                if isinstance(group, type(stack)):
                    mygroups.remove(group)
                    mybins[group] = tgt
                else:
                    target_parts.append(tgt)
            if target_parts:
                mygroups = mygroups[0:len(target_parts)]
                target = "__".join(target_parts)

        # Pass to standard
        ids = super().get_ids(stack, groups, mygroups, target)
        for bin in bins:
            ids = [
                "__".join((target, binid))
                for target in ids
                for binid in bin.stage.get_checkpoint_ids(bin, groups, target)
                if bin not in mybins or binid == mybins[bin]
            ]
        return ids


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

        self.regex = f"({self.key}\\d+|)"

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
