"""Pipelines Module

Contains classes for pre-configured pipelines comprising multiple
stages.
"""

import logging
import os

from collections import OrderedDict
from collections.abc import Mapping
from typing import Dict, List, Set, Optional, Tuple

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
                stage_params = stage.parse(stage_name)
                for param in stage.params:
                    try:
                        default = stage_params.get(param.name, param.default)
                        self.add_param(param.key, param.type_name, param.name, param.value, default)
                        params.setdefault(stage_path, []).append(param.name)
                    except YmpRuleError:
                        pass
            self._params = params
        return super().params

    def get_path(self, stack, typ=None, pipeline=None, caller=None):
        pipeline_parameters = self.parse(stack.stage_name)
        param_map = {
            key.format(**pipeline_parameters): value
            for key, value in self._params.items()
        }
        if typ is not None:
            pipeline = self.outputs[typ]
        if pipeline is None:
            pipeline = self.pipeline
        pipeline = pipeline.format(**pipeline_parameters)
        stages = []
        path = ""
        for stage_name in pipeline.lstrip(".").split("."):
            path = ".".join((path, stage_name))
            takes_params = param_map.get(path)
            if not takes_params:
                stages.append(stage_name)
                continue

            stage = find_stage(stage_name)
            stage_parameters = stage.parse(stage_name)
            for param in takes_params:
                if param in pipeline_parameters:
                    stage_parameters[param] = pipeline_parameters[param]
            stages.append(stage.format(stage_parameters))

        prefix = stack.name.rsplit(".", 1)[0]
        return ".".join([prefix]+stages)

    def _make_outputs(self) -> Dict[str, List[Tuple[str,bool]]]:
        """Collects outputs from all stages within pipeline

        Returns: { suffix: (stack_suffix, is_hidden) }
        """
        outputs = {}
        for stage_path, cfg in self.stages.items():
            stage_name = stage_path.rsplit(".", 1)[-1]
            stage = find_stage(stage_name)
            ourhide = cfg.get("hide", self.hide_outputs)
            for output, pathlist in stage.get_outputs(stage_path).items():
                ourpathlist = outputs.setdefault(output, [])
                for path, hide in pathlist:
                    ourpathlist.append((path, hide|ourhide))
        return outputs

    def get_outputs(self, path: str) -> Dict[str, List[Tuple[str,bool]]]:
        """Returns a dictionary of outputs"""
        if self._outputs is None:
            self._outputs = self._make_outputs()
        path, _, _, = path.rpartition("." + self.name)
        return {
            output: [(path + lpath, hidden) for lpath, hidden in pathlist]
            for output, pathlist in self._outputs.items()
        }


    @property
    def outputs(self) -> Dict[str, str]:
        """The outputs of a pipeline are the sum of the outputs
        of each component stage. Outputs of stages further down
        the pipeline override those generated earlier.
        """
        if self._outputs is None:
            self._outputs = self._make_outputs()
        res = {}
        for output, pathlist in self._outputs.items():
            for path, hidden in reversed(pathlist):
                if hidden:
                    continue
                res[output] = path
                break
        return res

    def can_provide(self, inputs: Set[str], full_stack: bool = False) -> Dict[str, str]:
        """Determines which of ``inputs`` this stage can provide.

        The result dictionary values will point to the "real" output.
        """
        if full_stack:
            if self._outputs is None:
                self._outputs = self._make_outputs()

            res = {
                output: pathlist
                for output, pathlist in self._outputs.items()
                if output in inputs
            }
        else:
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
