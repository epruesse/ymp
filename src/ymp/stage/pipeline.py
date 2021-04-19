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
from ymp.exceptions import YmpConfigError, YmpRuleError


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
        self._params = None
        self._outputs: Optional[Dict[str, str]] = None

        #: If true, outputs of stages are hidden by default
        self.hide_outputs = getattr(cfg, "hide", False)
        if 'params' in cfg and cfg.params is not None:
            if not isinstance(cfg.params, Mapping):
                raise YmpConfigError(cfg, "Params must contain a mapping", key="params")
            self._init_params(cfg.params)

        #: Dictionary of stages with configuration options for each
        self.stages = OrderedDict()
        path = ""
        if not "stages" in cfg:
            raise YmpConfigError(cfg, "Pipeline must have stages entry")
        for stage in cfg.stages:
            if stage is None:
                raise YmpConfigError(self, f"Empty stage name in pipeline '{name}'")
            if isinstance(stage, str):
                stage_name = stage
                stage_cfg = {}
            else:
                stage_name = next(iter(stage))
                stage_cfg = stage[stage_name]
            path = ".".join((path, stage_name))
            self.stages[path] = stage_cfg

        #: Path fragment describing this pipeline
        self.pipeline = path

    def _init_params(self, params):
        for param, data in params.items():
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

    @property
    def params(self):
        if self._params is None:
            params = {}
            for stage_path in self.stages:
                stage_name = stage_path.rsplit(".", 1)[-1]
                if "{" in stage_name:
                    # Cannot inherit params from stages with param wildcard in name
                    continue
                stage = find_stage(stage_name)
                if not isinstance(stage, Parametrizable):
                    continue
                for param in stage.params:
                    try:
                        self.add_param(param.key, param.type_name, param.name, param.value, param.default)
                        params.setdefault(stage_path, []).append(param.name)
                    except YmpRuleError:
                        pass
            self._params = params
        return super().params

    def get_path(self, stack, typ=None):
        prefix = stack.name.rsplit(".", 1)[0]
        if typ is None:
            suffix = self.pipeline
        else:
            suffix = self.outputs[typ]
        params = self.parse(stack.stage_name)
        suffix = suffix.format(**params)
        stages = []
        path = ""
        for stage_name in suffix.lstrip(".").split("."):
            path = ".".join((path, stage_name))
            takes_params = self._params.get(path)
            if not takes_params:
                stages.append(stage_name)
                continue
            stage = find_stage(stage_name)
            stage_params = stage.parse(stage_name)
            for param in takes_params:
                if param in params:
                    stage_params[param] = params[param]
            stages.append(stage.format(stage_params))
        return ".".join([prefix]+stages)

    def _make_outputs(self) -> Dict[str, str]:
        outputs = {}
        for stage_path, cfg in self.stages.items():
            if cfg.get("hide", self.hide_outputs):
                continue
            stage_name = stage_path.rsplit(".", 1)[-1]
            stage = find_stage(stage_name)
            new_outputs = stage.get_outputs(stage_path)
            outputs.update(new_outputs)
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
