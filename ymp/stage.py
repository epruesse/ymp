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
from inspect import getframeinfo, stack

from ymp.exceptions import YmpException
from ymp.snakemake import BaseExpander, ExpandableWorkflow
from ymp.util import AttrDict

if TYPE_CHECKING:
    from typing import List
    from snakemake.rules import Rule

log = logging.getLogger(__name__)


class YmpStageError(YmpException):
    """
    Exception raised if something went wrong creating a stage.

    Currently, cases are duplicating stage names or trying to
    enter two stages at the same time.
    """
    def __init__(self, stage: 'Stage', msg: str) -> None:
        msg = "Error in stage '{}': {}".format(stage, msg)
        super().__init__(msg)


class Stage(object):
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

    @classmethod
    def get_stages(cls):
        """
        Return all stages created within current workflow
        """
        # We need to store the Stages in the Workflow so that
        # they get deleted with the workflow. (Otherwise we'd run into
        # duplicate stage creation if snakemake() is called twice and
        # the same snakefiles parsed and loaded again).
        workflow = ExpandableWorkflow.global_workflow
        if not hasattr(workflow, "ymp_stages"):
            workflow.ymp_stages = AttrDict()
        return workflow.ymp_stages

    def __init__(self, name: str, altname: str=None) -> None:
        """
        Args:
            name: Name of this stage
            altname: Alternate name of this stage (used for stages with
                multiple output variants, e.g. filter_x and remove_x.

        """
        # Stage name
        self.name: str = name 
        # Alternate stage name
        self.altname: str = altname
        # Rules in this stage
        self.rules: List[Rule] = []
        # Documentation
        self.docstring: str = ""

        caller = getframeinfo(stack()[1][0])
        #: str: Name of file in which stage was defined
        self.filename = caller.filename
        #: int: Line number of stage definition
        self.lineno = caller.lineno

        stages = Stage.get_stages()
        if name in stages:
            raise YmpStageError(self, "Duplicate stage name")
        else:
            stages[name] = self
            if altname:
                stages[altname] = self

    def doc(self, doc: str) -> None:
        """Add documentation to Stage

        Args:
          doc: Docstring passed to Sphinx
        """
        self.docstring = doc

    def __enter__(self):
        if Stage.active is not None:
            raise YmpStageError(
                self.name,
                "Stage {} already active".format(self.active.name)
            )

        Stage.active = self
        return self

    def __exit__(self, *args):
        Stage.active = None

    def __str__(self):
        if self.altname:
            return "|".join((self.name, self.altname))
        return self.name

    def _add_rule(self, rule):
        rule.ymp_stage = self
        self.rules.append(rule)


class StageExpander(BaseExpander):
    """
    Registers rules with stages when they are created
    """
    def expand(self, rule, item):
        if Stage.active:
            Stage.active._add_rule(rule)
