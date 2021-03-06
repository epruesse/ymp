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
        path = ""
        for stage in cfg.stages:
            if isinstance(stage, str):
                path = ".".join((path, stage))
                self.stages[path] = {}
            else:
                stage_name = next(iter(stage))
                path = ".".join((path, stage_name))
                self.stages[path] = stage[stage_name]
        #: Path fragment describing this pipeline
        self.pipeline = path

    def _make_outputs(self) -> Dict[str, str]:
        outputs = {}
        for stage_path, cfg in self.stages.items():
            stage_name = stage_path.rsplit(".", 1)[-1]
            stage = find_stage(stage_name)
            if not cfg.get("hide", self.hide_outputs):
                outputs.update(stage.get_outputs(stage_path))
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
        realstack = stack.instance(self.get_path(stack))
        targets.extend(realstack.stage.get_all_targets(realstack))
        return targets

    def get_group(
            self,
            stack: "StageStack",
            default_groups: List[str],
            override_groups: List[str],
    ) -> List[str]:
        realstack = stack.instance(self.get_path(stack))
        return realstack.group

    def get_ids(self, stack, groups, mygroups=None, target=None):
        realstack = stack.instance(self.get_path(stack))
        return realstack.stage.get_ids(realstack, groups, mygroups, target)
