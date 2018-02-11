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

from ymp.util import AttrDict
from ymp.exceptions import YmpException
from ymp.snakemake import ExpandableWorkflow

log = logging.getLogger(__name__)


class YmpStageError(YmpException):
    def __init__(self, stage: 'Stage', msg: str):
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

    def __init__(self, name: str, altname:str=None):
        """
        Args:
            name: Name of this stage
            altname: Alternate name of this stage (used for stages with
                multiple output variants, e.g. filter_x and remove_x.

        """
        self.name = name
        self.altname = altname

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
        self.doc = doc

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
