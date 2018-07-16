import logging
import os

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


@pytest.fixture()
def persistent_demo_dir(tmpdir_factory, invoker_nodir, scope="module"):
    tmpdir = tmpdir_factory.mktemp("test_stages")
    with tmpdir.as_cwd():
        invoker_nodir.call("init", "demo")
    yield tmpdir


@pytest.mark.parametrize("target", targets)
def test_stage_dryrun(invoker_nodir, target, persistent_demo_dir):
    with persistent_demo_dir.as_cwd():
        invoker_nodir.call("make", "-n", target)

