import logging
from pathlib import Path

import pytest

from .data import targets


log = logging.getLogger(__name__)


@pytest.fixture(params=list(targets.values()), ids=list(targets.keys()))
def target(request, project_dir):
    with project_dir.as_cwd():
        from ymp.config import icfg
        icfg.init()
        for ds in icfg:
            yield request.param.format(ds)


def dump_logs(path=None):
    if path is None:
        path = Path.cwd()
    for f in path.iterdir():
        log.debug(f.name)
        if f.is_dir() and not f.name.startswith("."):
            dump_logs(path=f)
        elif f.name.endswith(".log"):
            log.error("Dumping logfile %s", f.name)
            with open(f) as fp:
                for line in fp:
                    log.error("log: "+line.rstrip())


@pytest.mark.parametrize("project_dir", ['toy'], indirect=True)
def test_run_rules(target):
    from click.testing import CliRunner
    from ymp.cmd import make as ymp_make
    runner = CliRunner()
    result = runner.invoke(ymp_make, ["-j2", target])
    if result.exit_code == 1:
        dump_logs()
    assert result.exit_code == 0, result.output
