import logging
import os

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


# Inject executables into PATH
# ==============================

@pytest.fixture()
def bin_dir(saved_tmpdir):
    binpath = os.path.join(saved_tmpdir, "bin")
    try:
        os.mkdir(binpath)
    except FileExistsError:
        if not os.path.isdir(binpath):
            raise
    path = os.environ['PATH']
    os.environ['PATH'] = ':'.join((binpath, path))
    yield binpath
    os.environ['PATH'] = path


class MockCmd(object):
    _calls = None

    def __init__(self, bin_dir, name, code=""):
        self.filename = os.path.join(bin_dir, name)
        self.logname = self.filename + "_cmd.log"
        content = "\n".join([
            '#!/bin/sh',
            'echo "$0 $@" >> "{}"'.format(self.logname),
            '\n'
        ])
        with open(self.filename, "w") as f:
            f.write(content)
            f.write(code)
        os.chmod(self.filename, 0o500)

    @property
    def calls(self):
        if not os.path.exists(self.logname):
            log.debug("%s is empty", self.logname)
            return []
        with open(self.logname) as r:
            data = r.read().splitlines()
        log.debug("%s:\n |  %s", self.logname, "\n | ".join(data))
        return data


@pytest.fixture
def mock_conda(bin_dir):
    yield MockCmd(bin_dir, "conda", "\n".join([
        'cmd=""',
        'while [ -n "$1" ]; do',
        '  case $1 in',
        '  --version)   echo conda 4.2; exit 0;;',
        '  --prefix|-p) shift; p="$1";;',
        '  --file|-f)   shift; f="$1";;'
        '  *)           cmd="$cmd $1";;',
        '  esac',
        '  shift',
        'done',
        'if [ x"$cmd" = x" env create" -a -n "$p" ]; then',
        '  mkdir "$p"',
        'fi',
        'if [ x"$cmd" = x" env export" -a -n "$p" ]; then',
        '  echo "dependencies: [one, two]"',
        'fi',
    ]))


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
    "Parametrizable project; defaults to 'toy'"
    return getattr(request, 'param', "toy")


@pytest.fixture()
def project_dir(request, project, saved_tmpdir):
    """Populated project directory

    parametrize `project` to get different project dirs
    """
    data_dir = py.path.local(__file__).dirpath('data', project)
    data_dir.copy(saved_tmpdir)
    log.info("Created project directory {}".format(saved_tmpdir))
    yield saved_tmpdir
    log.info("Tearing down project directory {}".format(saved_tmpdir))


@pytest.fixture()
def target(request, project_dir):
    with project_dir.as_cwd():
        log.info("Switched to directory {}".format(project_dir))
        import ymp.config as c
        c.icfg.init(force=True)
        targets = [request.param.format(ds) for ds in c.icfg]
        with open("target.txt", "w") as out:
            out.write("\n".join(targets))
        yield from targets


# Call into CLI
# =============

class Invoker(object):
    """Wrap invoking shell command

    Handles writing of out.log and cmd.sh as well as reloading ymp config
    on each call.
    """
    def __init__(self):
        from click.testing import CliRunner
        self.runner = CliRunner()
        from ymp.cli import main
        self.main = main
        self.initialized = False

    @property
    def icfg(self):
        from ymp.config import icfg
        return icfg

    def call(self, *args, standalone_mode=False, **kwargs):
        """Call into YMP CLI

        ``standalone_mode`` defaults to False so that exceptions are
        passed rather than caught.

        """
        if not self.initialized:
            self.icfg.init(force=True)
            self.initialized = True

        if not os.path.exists("cmd.sh"):
            with open("cmd.sh", "w") as f:
                f.write("#!/bin/bash -x\n")

        with open("cmd.sh", "w") as f:
            f.write(f"PATH={os.environ['PATH']} ymp {' '.join(args)}\n")

        result = self.runner.invoke(self.main, args, **kwargs,
                                    standalone_mode=standalone_mode)

        with open("out.log", "w") as f:
            f.write(result.output)

        if result.exception and not standalone_mode:
            raise result.exception

        return result

    def call_raises(self, *args, **kwargs):
        return self.call(*args, standalone_mode=True, **kwargs)


@pytest.fixture()
def invoker(saved_cwd):
    # Snakemake 4.7 waits 10 seconds during shutdown of cluster submitted
    # worklows -- unless this is set:
    os.environ['CIRCLECI'] = "true"

    return Invoker()
