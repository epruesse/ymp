import logging

from ymp.stage import StageStack
from ymp.exceptions import YmpConfigError

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class Pipeline(object):
    """
    Represents a subworkflow or pipeline
    """


    def __init__(self, pipeline, cfg):
        self.name = pipeline
        self.stages = cfg
        self.stagestack = StageStack.get(".".join(self.stages))

        self.output_map = {}
        stage_names = list(self.stages)
        while stage_names:
            path = ".".join(stage_names)
            stage = self.stagestack._find_stage(stage_names.pop())
            if not stage:
                continue
            for fn in stage.outputs:
                if fn not in self.output_map:
                    self.output_map[fn] = path

        self.inputs = set()

    @property
    def project(self):
        import ymp
        cfg = ymp.get_config()
        if self.stages[0] in cfg.projects:
            return cfg.projects[self.stages[0]]
        if self.stages[0] in cfg.pipelines:
            return cfg.pipelines[self.stages[0]].project
        raise YmpConfigError(self.stages, "Pipeline must start with project")

    def __str__(self):
        return self.name

    def __repr__(self):
        return f"{self.__class__.__name__} {self!s}"

    def get_path(self, suffix=None):
        return self.name

    @property
    def outputs(self):
        return list(self.output_map.keys())

    @property
    def stamp(self):
        return self.dir + "/all_targets.stamp"

    def can_provide(self, inputs):
        return inputs.intersection(self.outputs)

    def get_inputs(self):
        return set()
