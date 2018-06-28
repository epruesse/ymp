import logging

import pytest

from ymp.stage import Stage

log = logging.getLogger(__name__)

stages = Stage.get_registry()

ympmakedoc = ">>> ymp make "
stage_testcount = {stage: 0 for stage in stages}
targets = []
for stage in stages.values():
    for line in stage.docstring.splitlines():
        if line.strip().startswith(ympmakedoc):
            stage_testcount[stage.name] += 1
            target = line.strip()[len(ympmakedoc):]
            targets.append(target)


@pytest.mark.parametrize("target", targets)
def test_stage_dryrun(invoker, target):
    print(target)

