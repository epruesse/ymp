import logging
from pathlib import Path

import pytest

from .data import targets

del targets['phyloFlash'] ## can't work with our test data

log = logging.getLogger(__name__)


@pytest.fixture(params=list(targets.values()), ids=list(targets.keys()))
def target(request, project_dir):
    with project_dir.as_cwd():
        from ymp.config import icfg
        icfg.init()
        for ds in icfg:
            yield request.param.format(ds)



@pytest.mark.parametrize("project_dir", ['toy'], indirect=True)
def test_run_rules(target):
    from click.testing import CliRunner
    from ymp.cmd import make as ymp_make
    runner = CliRunner()
    result = runner.invoke(ymp_make, ["-j2", target])
    if result.exit_code != 0:
        for line in result.output.splitlines():
            log.error("out: %s", line)
    assert result.exit_code == 0, result.output
