"""Pipelines Module

Contains classes for pre-configured pipelines comprising multiple
stages.
"""

import logging
import os

from typing import Dict, List, Set

from ymp.stage import StageStack, find_stage
from ymp.stage.base import ConfigStage
from ymp.exceptions import YmpConfigError


log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class Pipeline(ConfigStage):
    """
    A virtual stage aggregating a sequence of stages, i.e. a pipeline
    or sub-workflow.

    Pipelines are configured via ``ymp.yml``.

    Example:
        pipelines:
            my_pipeline:
              - stage_1
              - stage_2
              - stage_3
    """
    def __init__(self, name: str, cfg: List[str]) -> None:
        super().__init__(name, cfg)
        self.stage_names: List[str] = cfg
        self._outputs: Dict[str, str] = None

    @property
    def outputs(self) -> Dict[str, str]:
        """The outputs of a pipeline are the sum of the outputs
        of each component stage. Outputs of stages further down
        the pipeline override those generated earlier.

        TODO: Allow hiding the output of intermediary stages.
        """
        outputs = {}
        path = ""
        for stage_name in self.stage_names:
            stage = find_stage(stage_name)
            path = ".".join((path, stage_name))
            stage_outputs = stage.outputs
            if isinstance(stage_outputs, set):
                outputs.update({output: path for output in stage_outputs})
            else:
                outputs.update(stage_outputs)
        return outputs

    def can_provide(self, inputs: Set[str]) -> Dict[str, str]:
        """Determines which of ``inputs`` this stage can provide.

        The result dictionary values will point to the "real" output.
        """
        return {
            output: path
            for output, path in self.outputs.items()
            if output in inputs
        }

    @property
    def pipeline(self):
        return "." + ".".join(self.stage_names)

    def get_path(self, stack):
        prefix = stack.name.rsplit('.',1)[0]
        return prefix + self.pipeline

    def get_all_targets(self, stack):
        targets = []
        # First add the symlink for ourselves, but only if it
        # does not exist yet, due to a bug in Snakemake 5.20.1.
        if not os.path.exists(stack.name):
            targets += [stack.name]
        # Now add the target the last part of the pipeline
        # points to.
        realstack = stack.get(self.get_path(stack))
        targets.extend(realstack.stage.get_all_targets(realstack))
        return targets

