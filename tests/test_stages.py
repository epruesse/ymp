import logging

import pytest

from ymp.stage import Stage

log = logging.getLogger(__name__)


def get_targets():
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
    return targets


targets = get_targets()


@pytest.mark.parametrize("targetx", targets)
def test_stage_dryrun(invoker, targetx):
    invoker.call("init", "demo")
    invoker.call("make", "-n", targetx)


@pytest.mark.runs_tool
@pytest.mark.parametrize("targetx", targets)
def test_stage_run(invoker, targetx):
    invoker.call("init", "demo")
    invoker.call("make", targetx)
