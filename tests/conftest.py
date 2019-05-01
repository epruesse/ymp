import logging
import os
import shlex
import shutil

import py

import pytest

import ymp
import ymp.config

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
        # delete conda / conda_archive before saving tmpdir
        for path in ("conda", "conda_archive"):
            delpath = tmpdir.join(path)
            if delpath.check(exists=1):
                delpath.remove(rec=True)
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
def mock_cmd(request, bin_dir):
    cmd, script = request.param
    print("###", cmd, script)
    yield MockCmd(bin_dir, cmd, script)


@pytest.fixture
def mock_conda(bin_dir):
    base_dir = os.path.dirname(bin_dir)
    yield MockCmd(bin_dir, "conda", "\n".join([
        'cmd=""',
        'while [ -n "$1" ]; do',
        '  case $1 in',
        '  --version)   echo conda 4.2; exit 0;;',
        '  --prefix|-p) shift; p="$1";;',
        '  --file|-f)   shift; f="$1";;'
        '  --json)      shift; j=Y;;'
        '  *)           cmd="$cmd $1";;',
        '  esac',
        '  shift',
        'done',
        'if echo "$cmd" |grep -q " create "; then',
        '  mkdir -p "$p"',
        'fi',
        'if [ x"$cmd" = x" env export" -a -n "$p" ]; then',
        '  echo "dependencies: [one, two]"',
        'fi',
        'if [ x"$cmd" = x" info" ]; then',
        '  echo \'{{"conda_prefix": "{}"}}\''.format(base_dir),
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
def demo_dir(invoker, saved_cwd):
    invoker.call("init", "demo")
    return saved_cwd


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
        ymp.get_config().unload()
        cfg = ymp.get_config()
        targets = [request.param.format(prj) for prj in cfg.projects]
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
        self.toclean = []

    def call(self, *args, standalone_mode=False, **kwargs):
        """Call into YMP CLI

        ``standalone_mode`` defaults to False so that exceptions are
        passed rather than caught.

        """
        if not self.initialized:
            # change path to USER ymp config (default ~/.ymp/ymp.yml)
            # so that settings there do not interfere with tests
            ymp.config.ConfigMgr.CONF_USER_FNAME = "ymp_user.yml"
            # force reload
            ymp.get_config().unload()

        if not os.path.exists("cmd.sh"):
            with open("cmd.sh", "w") as f:
                f.write("#!/bin/bash -x\n")

        argstr = " ".join(shlex.quote(arg) for arg in args)
        with open("cmd.sh", "w") as f:
            f.write(f"PATH={os.environ['PATH']} ymp {argstr} \"$@\"\n")

        result = self.runner.invoke(self.main, args, **kwargs,
                                    standalone_mode=standalone_mode)

        with open("out.log", "w") as f:
            f.write(result.output)

        if result.exception and not standalone_mode:
            raise result.exception

        return result

    def call_raises(self, *args, **kwargs):
        return self.call(*args, standalone_mode=True, **kwargs)

    def clean(self):
        for toclean in self.toclean:
            shutil.rmtree(toclean, ignore_errors=True)


@pytest.fixture()
def invoker(saved_cwd):
    # Snakemake 4.7 waits 10 seconds during shutdown of cluster submitted
    # worklows -- unless this is set:
    os.environ['CIRCLECI'] = "true"

    invoker = Invoker()
    yield invoker
    invoker.clean()


@pytest.fixture()
def invoker_nodir():
    # Snakemake 4.7 waits 10 seconds during shutdown of cluster submitted
    # worklows -- unless this is set:
    os.environ['CIRCLECI'] = "true"

    invoker = Invoker()
    yield invoker
    invoker.clean()


@pytest.fixture(name="envvar")
def envvar_():
    to_restore = {}

    def envvar(var, value):
        if var in os.environ:
            to_restore[var] = os.environ[var]
        else:
            to_restore[var] = None
        os.environ[var] = value

    yield envvar

    for var, value in to_restore.items():
        if value is not None:
            os.environ[var] = value
        else:
            del os.environ[var]


class MockFileDownloader(object):
    def __init__(self, block_size=None, timeout=None, parallel=None,
                 loglevel=None, alturls=None):
        pass

    def get(self, urls, dest, md5s=None):
        return True


@pytest.fixture()
def mock_downloader():
    import ymp.download
    orig = ymp.download.FileDownloader
    ymp.download.FileDownloader = MockFileDownloader
    yield MockFileDownloader
    ymp.download.FileDownloader = orig
