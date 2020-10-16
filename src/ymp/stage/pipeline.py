"""Pipelines Module

Contains classes for pre-configured pipelines comprising multiple
stages.
"""

import logging
import os

from collections import OrderedDict
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
            hide: false
            stages:
              - stage_1:
                  hide: true
              - stage_2
              - stage_3
    """
    def __init__(self, name: str, cfg: List[str]) -> None:
        super().__init__(name, cfg)
        self._outputs: Dict[str, str] = None

        #: If true, outputs of stages are hidden by default
        self.hide_outputs = getattr(cfg, "hide", False)
        #: Dictionary of stages with configuration options for each
        self.stages = OrderedDict()
        for stage in cfg.stages:
            if isinstance(stage, str):
                self.stages[stage] = {}
            else:
                stage_name = next(iter(stage))
                self.stages[stage_name] = stage[stage_name]

    @property
    def stage_names(self):
        """Names of the stages comprising this pipeline"""
        return list(self.stages.keys())

    @property
    def pipeline(self):
        return "." + ".".join(self.stage_names)

    def _make_outputs(self) -> Dict[str, str]:
        outputs = {}
        path = ""
        for stage_name, cfg in self.stages.items():
            stage = find_stage(stage_name)
            path = ".".join((path, stage_name))
            if not cfg.get("hide", self.hide_outputs):
                outputs.update(stage.get_outputs(path))
        return outputs

    @property
    def outputs(self) -> Dict[str, str]:
        """The outputs of a pipeline are the sum of the outputs
        of each component stage. Outputs of stages further down
        the pipeline override those generated earlier.
        """
        if self._outputs is None:
            self._outputs = self._make_outputs()
        return self._outputs

    def can_provide(self, inputs: Set[str]) -> Dict[str, str]:
        """Determines which of ``inputs`` this stage can provide.

        The result dictionary values will point to the "real" output.
        """
        res = {
            output: path
            for output, path in self.outputs.items()
            if output in inputs
        }
        return res

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
