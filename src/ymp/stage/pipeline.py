"""Pipelines Module

Contains classes for pre-configured pipelines comprising multiple
stages.
"""

import logging
import os

from collections import OrderedDict
from collections.abc import Mapping
from typing import Dict, List, Set, Optional

from ymp.stage import StageStack, find_stage
from ymp.stage.base import ConfigStage
from ymp.stage.params import Parametrizable
from ymp.exceptions import YmpConfigError


log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class Pipeline(Parametrizable, ConfigStage):
    """
    A virtual stage aggregating a sequence of stages, i.e. a pipeline
    or sub-workflow.

    Pipelines are configured via ``ymp.yml``.

    Example:
        pipelines:
          my_pipeline:
            hide: false
            params:
              length:
                key: L
                type: int
                default: 20
            stages:
              - stage_1{length}:
                  hide: true
              - stage_2
              - stage_3
    """
    def __init__(self, name: str, cfg) -> None:
        super().__init__(name, cfg)
        self._outputs: Optional[Dict[str, str]] = None

        #: If true, outputs of stages are hidden by default
        self.hide_outputs = getattr(cfg, "hide", False)

        if 'params' in cfg:
            if not isinstance(cfg.params, Mapping):
                raise YmpConfigError(cfg, "Params must contain a mapping", key="params")
            for param, data in cfg.params.items():
                if not isinstance(data, Mapping):
                    raise YmpConfigError(data, "Param must contain a mapping", key=param)
                try:
                    key = data['key']
                    typ = data['type']
                except KeyError as exc:
                    raise YmpConfigError(data, "Param must have at least key and type defined") from exc
                self.add_param(
                    key, typ, param, data.get("value"), data.get("default")
                )

        #: Dictionary of stages with configuration options for each
        self.stages = OrderedDict()
        path = ""
        for stage in cfg.stages:
            if stage is None:
                raise YmpConfigError(self, f"Empty stage name in pipeline '{name}'")
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
        params = self.parse(stack.stage_name)
        pipeline = self.pipeline.format(**params)
        return prefix + pipeline

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
            default_groups: List[str]
    ) -> List[str]:
        realstack = stack.instance(self.get_path(stack))
        return realstack.stage.get_group(realstack, default_groups)

    def get_ids(self, stack, groups, mygroups=None, target=None):
        realstack = stack.instance(self.get_path(stack))
        return realstack.stage.get_ids(realstack, groups, mygroups, target)
