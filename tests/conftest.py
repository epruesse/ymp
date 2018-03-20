import logging

import py

import pytest

log = logging.getLogger(__name__)


# Add pytest options
# ==================

def pytest_addoption(parser):
    parser.addoption("--run-tools", action="store_true",
                     default=False, help="Skip tests running tools")
    parser.addoption("--cwd-save-dir", metavar="DIR",
                     default="test_failures",
                     help="""Tests needing local files are run in a temporary
                     directory. If a test fails, a copy of the directory is
                     made in this location.""")
    parser.addoption("--cwd-save-always", action="store_true",
                     default=False, help="Always save test CWD")


# Add pytest.mark.skip_tool
# =========================

def pytest_collection_modifyitems(config, items):
    if not config.getoption("--run-tools"):
        skip_tool = pytest.mark.skip(reason="Not running tools")
        for item in items:
            if "runs_tool" in item.keywords:
                item.add_marker(skip_tool)


# Allow executing tests in dir saved on error
# ===========================================

@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # execute all other hooks to obtain the report object
    outcome = yield
    rep = outcome.get_result()

    # set a report attribute for each phase of a call, which can
    # be "setup", "call", "teardown"
    setattr(item, "rep_" + rep.when, rep)


@pytest.fixture()
def saved_tmpdir(request, tmpdir):
    yield tmpdir
    if (
        request.config.getoption("--cwd-save-always")
        or not hasattr(request.node, 'rep_call')
        or request.node.rep_call.failed
    ):
        name_parts = request.node.name.replace("]", "").split("[")
        cwd_save_dir = request.config.getoption("--cwd-save-dir")
        destdir = py.path.local(cwd_save_dir).join(*name_parts)
        if destdir.check(exists=1):
            destdir.remove(rec=True)
        destdir.dirpath().ensure_dir()
        tmpdir.move(destdir)
        log.error("Saved failed test data to %s", str(destdir))
    else:
        tmpdir.remove()


@pytest.fixture()
def saved_cwd(saved_tmpdir):
    with saved_tmpdir.as_cwd():
        yield saved_tmpdir


# Activate this to get some profiling data while testing
# ======================================================

@pytest.fixture(scope="module")  # autouse=True)
def profiling():
    import yappi
    yappi.start()
    yield
    yappi.stop()
    profile = yappi.get_func_stats()
    profile.sort("subtime")
    with open("profile.txt", "w") as f:
        profile.print_all(out=f, columns={
            0: ("name", 120),
            1: ("ncall", 10),
            2: ("tsub", 8),
            3: ("ttot", 8),
            4: ("tavg", 8)})


# Provision CWD with files from data/<project>
# ============================================

@pytest.fixture()
def project(request):
    return request.param


@pytest.fixture()
def project_dir(request, project, saved_tmpdir):
    data_dir = py.path.local(__file__).dirpath('data', project)
    data_dir.copy(saved_tmpdir)
    log.info("Created project directory {}".format(saved_tmpdir))
    yield saved_tmpdir
    log.info("Tearing down project directory {}".format(saved_tmpdir))


@pytest.fixture()
def target(request, project_dir):
    with project_dir.as_cwd():
        log.info("Switched to directory {}".format(project_dir))
        from ymp.config import icfg
        icfg.init()
        targets = [request.param.format(ds) for ds in icfg]
        with open("target.txt", "w") as out:
            out.write("\n".join(targets))
        yield from targets


# Call into CLI
# =============

class Invoker(object):
    def __init__(self):
        from click.testing import CliRunner
        self.runner = CliRunner()
        from ymp.cli import main
        self.main = main

    def call(self, *args, **kwargs):
        from ymp.config import icfg
        icfg.init()
        result = self.runner.invoke(self.main, args, **kwargs,
                                    standalone_mode=False)
        with open("out.log", "w") as f:
            f.write(result.output)
        if result.exception:
            raise result.exception
        return result


@pytest.fixture()
def invoker(saved_cwd):
    # Snakemake 4.7 waits 10 seconds during shutdown of cluster submitted
    # worklows -- unless this is set:
    import os
    os.environ['CIRCLECI'] = "true"

    return Invoker()
