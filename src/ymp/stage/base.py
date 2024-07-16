"""
Base classes for all Stage types
"""

import logging
import os
import re

from typing import Set, Dict, Union, List, Optional, Tuple

from snakemake.rules import Rule
from snakemake.workflow import Workflow

from ymp.exceptions import YmpStageError, YmpRuleError, YmpException
from ymp.string import ProductFormatter
from ymp.yaml import MultiProxy


log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class BaseStage:
    """Base class for stage types"""

    def __init__(self, name: str) -> None:
        #: The name of the stage is a string uniquely identifying it
        #: among all stages.
        self.name = name

        #: Alternative name
        self.altname = None

        #: The docstring describing this stage. Visible via ``ymp
        #: stage list`` and in the generated sphinx documentation.
        self.docstring: Optional[str] = None

    def __str__(self) -> str:
        """Cast to string we just emit our name"""
        return self.name

    def __repr__(self):
        """Using `repr()` we emit the subclass as well as our name"""
        return f"{self.__class__.__name__}({self!s})"

    def doc(self, doc: str) -> None:
        """Add documentation to Stage

        Args:
          doc: Docstring passed to Sphinx
        """
        #: Docstring describing stage
        self.docstring = doc

    def match(self, name: str) -> bool:
        """Check if the ``name`` can refer to this stage

        As component of a `StageStack`, a stage may be identified by
        alternative names and may also be parametrized by suffix
        modifiers. Stage types supporting this behavior must override
        this function.

        """
        return name == self.name

    def get_inputs(self) -> Set[str]:
        # pylint: disable = no-self-use
        """Returns the set of inputs required by this stage

        This function must return a copy, to ensure internal data is
        not modified.

        """
        return set()

    @property
    def outputs(self) -> Union[Set[str], Dict[str, str]]:
        """Returns the set of outputs this stage is able to generate.

        May return either a `set` or a `dict` with the dictionary
        values representing redirections in the case of virtual stages
        such as `Pipeline` or `Reference`.

        """
        return set()

    def get_outputs(self, path: str) -> Dict[str, List[Tuple[str,bool]]]:
        """Returns a dictionary of outputs"""
        outputs = self.outputs
        return {
            output: [(path, False)]
            for output in self.outputs
        }

    def can_provide(self, inputs: Set[str], full_stack: bool = False) -> Dict[str, str]:
        """Determines which of ``inputs`` this stage can provide.

        Returns a dictionary with the keys a subset of ``inputs`` and
        the values identifying redirections. An empty string indicates
        that no redirection is to take place. Otherwise, the string is
        the suffix to be appended to the prior `StageStack`.

        """
        return {
            output: [("",False)] if full_stack else ""
            for output in inputs.intersection(self.outputs)
        }

    def get_path(self, stack: "StageStack", typ = None, pipeline = None) -> str:
        # pylint: disable = no-self-use
        """On disk location for this stage given ``stack``.

        Called by `StageStack` to determine the real path for
        virtual stages (which must override this function).
        """
        return stack.name

    def get_all_targets(self, stack: "StageStack", output_types=None) -> List[str]:
        """Targets to build to complete this stage given ``stack``.

        Typically, this is the StageStack's path appended with the
        stamp name.
        """
        if output_types is None:
            output_types = [
                output for output in self.outputs
                if "{sample}" in output
                and not "{:bin:}" in output
            ]
        targets = stack.targets
        path = stack.path
        output_files = [
            path + output_type.format(sample=target)
            for output_type in output_types
            for target in targets
        ]
        return output_files

    def get_group(
            self,
            stack: "StageStack",
            default_groups: List[str],
    ) -> List[str]:
        """Determine output grouping for stage

        Args:
          stack: The stack for which output grouping is requested.
          default_groups: Grouping determined from stage inputs
          override_groups: Override grouping from GroupBy stage or None.
        """
        if stack.prev_stack is not None:
            if stack.prev_stack.stage.modify_next_group(stack.prev_stack):
                raise YmpStageError(f"Cannot override {self} grouping")
        return default_groups

    def modify_next_group(self, _stack: "StageStack"):
        # pylint: disable = no-self-use
        return None

    def get_ids(
            self,
            stack: "ymp.stage.StageStack",
            groups: List[str],
            match_groups: Optional[List[str]] = None,
            match_value: Optional[str] = None,
    ) -> List[str]:
        # pylint: disable = no-self-use
        """Determine the target ID names for a set of active groupings

        Called from ``{:target:}`` and ``{:targets:}``. For ``{:targets:}``,
        ``groups`` is the set of active groupings for the stage
        stack. For ``{:target:}``, it's the same set for the source of
        the file type, the current grouping and the current target.

        Args:
          groups: Set of columns the values of which should form IDs
          match_value: Limit output to rows with this value
          match_groups: ... in these groups
        """
        # empty groups means single output file, termed ALL
        if not groups:
            return ['ALL']
        if match_value == 'ALL':
            match_value = None
            match_groups = None
        if not match_groups and match_value:
            return [match_value]
        # Fastpath: If groups and match groups are identical the input
        # and output IDs must be identical.
        if groups == match_groups:
            return [match_value]
        # Pass through to project
        return stack.project.do_get_ids(stack, groups, match_groups, match_value)

    def has_checkpoint(self) -> bool:
        # pylint: disable = no-self-use
        return False

class Activateable:
    """Mixin for Stages that can be filled with rules from Snakefiles.

    There can be only one active stage across all classes deriving
    from this.
    """
    #: Currently active stage ("entered")
    _active: Optional[BaseStage] = None

    @staticmethod
    def get_active() -> BaseStage:
        return Activateable._active

    @staticmethod
    def set_active(stage: Optional[BaseStage]) -> None:
        Activateable._active = stage

    def __init__(self, *args, **kwargs) -> None:
        #: Rules in this stage
        self.rules: List[Rule] = []
        self._last_rules: List[Rule] = []
        super().__init__(*args, **kwargs)

    def __enter__(self) -> "Activateable":
        if self.get_active() is not None:
            raise YmpRuleError(
                self,
                f"Failed to enter stage '{self}', "
                f"already in stage {self.get_active()}'."
            )

        self.set_active(self)
        self._last_rules = self.rules.copy()
        return self

    def __exit__(self, *args) -> None:
        self.set_active(None)

    def add_rule(self, rule: "Rule", workflow: "Workflow") -> None:
        rule.ymp_stage = self
        self.rules.append(rule.name)
        if self._last_rules:
            for lastrule in self._last_rules:
                workflow.ruleorder(rule.name, lastrule)

    def check_active_stage(self, name: str) -> None:
        if not self.get_active():
            raise YmpException(
                f"Use of {{:{name}:}} requires active Stage"
            )
        if not self.get_active() == self:
            raise YmpException(
                f"Internal error: {self} running but {self.get_active()} active."
            )

    def register_inout(self, name: str, target: Set, item: str) -> None:
        """Determine stage input/output file type from prev/this filename

        Detects patterns like "PREFIX{: NAME :}/INFIX{TARGET}.EXT".
        Also checks if there is an active stage.

        Args:
           name: The NAME
           target: Set to which to add the type
           item: The filename
        Returns:
           Normalized output pattern
        """
        self.check_active_stage(name)
        match = re.fullmatch(r"""
        (?P<prefix>.*)\{{:\s*{name}\s*:\}}
        (?P<infix>/?.*?)
        (?P<target>\{{:?\s*(?:target|sample|source)(?:|\([^)]*\))\s*:?}})?
        (?P<suffix>.*)
        """.format(name=name), item, re.VERBOSE)
        if not match:
            raise YmpRuleError(self, f"Malformed '{{:{name}:}}' string: '{item}'")
        parts = match.groupdict()
        prefix = parts["prefix"]
        if parts.get("prefix"):
            raise YmpRuleError(self, f"Stage prefix '{prefix}' in '{item}' not supported")
        infix = parts["infix"]
        if infix and infix != "/":
            raise YmpRuleError(self, f"Filename prefix '{infix}' in '{item}' not supported")
        suffix = parts["suffix"]
        if parts["target"]:
            normtype = "/{sample}" + suffix
        else:
            normtype = "/" + suffix
        if not "{" in suffix:
            target.add(normtype)
        return normtype


class ConfigStage(BaseStage):
    """Base for stages created via configuration

    These Stages derive from the ``yml.yml`` and not from a rules file.
    """
    def __init__(self, name: str, cfg: 'MultiProxy'):
        #: Semi-colon separated list of file names defining this Stage.
        self.filename = ';'.join(cfg.get_files())
        #: Line number within the first file at which this Stage is defined.
        self.lineno = next(iter(cfg.get_linenos()), None)
        super().__init__(name)
        #: The configuration object defining this Stage.
        self.cfg = cfg

    @property
    def defined_in(self):
        """List of files defining this stage
        
        Used to invalidate caches.
        """
        return self.cfg.get_files()
