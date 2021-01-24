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

import logging
from typing import List

from ymp.stage.base import BaseStage
from ymp.exceptions import YmpStageError


log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class GroupBy(BaseStage):
    """Virtual stage for grouping"""
    PREFIX = "group_"
    def __init__(self, name: str) -> None:
        super().__init__(name)

    def modify_next_group(
            self,
            stack: "StageStack",
    ) -> List[str]:
        name = stack.stage_names[-1]
        if not self.match(name):
            raise YmpStageError(f"Internal Error: {name} not a group?")

        # fetch directly previous grouoing
        if stack.prev_stack is not None:
            group = stack.prev_stack.stage.modify_next_group(stack.prev_stack) or []
        else:
            group = []

        group_name = name[len(self.PREFIX):]
        if group_name == "ALL":
            if group:
                raise YmpStageError("Regrouping to ALL means previous group statement has no effect")
        elif group_name == "BIN":
            group += ["__bin__"]
        else:
            group += [group_name]
        return group

    def get_group(
            self,
            stack: "StageStack",
            default_groups: List[str],
    ) -> List[str]:
        return []

    def match(self, name: str) -> bool:
        return name.startswith(self.PREFIX)
