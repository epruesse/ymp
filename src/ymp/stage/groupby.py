"""Implements forward grouping

Grouping allows processing multiple input datasets at once, such as in
a co-assembly. It is initiated by adding the virtual stage
"group_<COL>" directly before the stage that should be grouping its
output. "<COL>" may be a project data column, in which case all data
for which column COL shares a value will be combined, or "ALL", which
combines all samples. The output filename prefix will be either the
column value or "ALL".

>>> ymp make mock.group_sample.assemble_megahit
>>> ymp make mock.group_ALL.assemble_megahit

Subsequent stages will use the most finegrained grouping required by
their input data.

# FIXME: How to avoid re-specifying groupby?

"""

from typing import List

from ymp.stage.base import BaseStage

class GroupBy(BaseStage):
    """Dummy stage for grouping"""
    PREFIX = "group_"
    def __init__(self, name: str) -> None:
        super().__init__(name)

    def get_group(self, stack: "StageStack") -> List[str]:
        for name in reversed(stack.stage_names):
            if self.match(name):
                return name[len(self.PREFIX):]

    def match(self, name: str) -> bool:
        return name.startswith(self.PREFIX)
