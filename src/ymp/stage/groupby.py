from ymp.stage.base import BaseStage

class GroupBy(BaseStage):
    """Dummy stage for grouping"""
    def __init__(self, name):
        self.name = name

    def get_path(self):
        # group_xxx does not have a path
        return None

    def can_provide(self, inputs):
        # group_xxx does not privide anything
        return set()

    @property
    def outputs(self):
        return set()

