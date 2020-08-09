"""Pipelines Module

Contains classes for pre-configured pipelines comprising multiple
stages.
"""

import logging
import os

from ymp.stage import StageStack, find_stage
from ymp.stage.base import ConfigStage
from ymp.exceptions import YmpConfigError


log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class Pipeline(ConfigStage):
    """
    Represents a subworkflow or pipeline
    """
    def __init__(self, name, cfg):
        super().__init__(name, cfg)
        self.stage_names = cfg
        self.output_map = {}
        path = ""
        for stage_name in cfg:
            stage = find_stage(stage_name)
            path = ".".join((path, stage_name))
            for output in stage.outputs:
                self.output_map[output] = path
        self.pipeline = path

    def get_path(self, stack):
        prefix = stack.name.rsplit('.',1)[0]
        return prefix + self.pipeline

    def get_all_targets(self, stack):
        if os.path.exists(stack.name):
            # WORKAROUND:
            # If the symlink to the output of the pipeline already exists,
            # Snakemake raises an unnecessary ChildIOException because
            # the symlink gets resolved to the pipeline output directory
            # before checking that no output is contained in another rule's
            # output. See snakemake.dag.DAG.check_directory_output.
            return super().get_all_targets(stack)
        return super().get_all_targets(stack) + [stack.name]

    @property
    def outputs(self):
        return list(self.output_map.keys())

