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

from ymp.exceptions import YmpRuleError
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
        return "".join([
            self.prev,
            "{_YMP_VRT}{_YMP_ASM}.",
            Stage.active.name
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

        return super().expand(rule, item, **kwargs)

    def expands_field(self, field):
        return field not in 'func'

    class Formatter(ColonExpander.Formatter, PartialFormatter):
        def get_value(self, key, args, kwargs):
            stage = Stage.active
            if hasattr(stage, key):
                return getattr(stage, key)
            return super().get_value(key, args, kwargs)
