import logging

import py

import pytest

log = logging.getLogger(__name__)


# from docs; add "rep_setup", "rep_call" and "rep_teardown"
# to request.node
@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # execute all other hooks to obtain the report object
    outcome = yield
    rep = outcome.get_result()

    # set a report attribute for each phase of a call, which can
    # be "setup", "call", "teardown"
    setattr(item, "rep_" + rep.when, rep)


@pytest.fixture()
def project(request):
    return request.param


@pytest.fixture()
def project_dir(request, project, tmpdir):
    data_dir = py.path.local(__file__).dirpath('data', project)
    data_dir.copy(tmpdir)
    log.info("Created project directory {}".format(tmpdir))
    yield tmpdir
    if not hasattr(request.node, 'rep_call') or request.node.rep_call.failed:
        name_parts = request.node.name.replace("]", "").split("[")
        destdir = py.path.local('test_failures').join(*name_parts)
        if destdir.check(exists=1):
            destdir.remove(rec=True)
        destdir.dirpath().ensure_dir()
        tmpdir.move(destdir)
        log.error("Saved failed test data to %s", str(destdir))
    else:
        tmpdir.remove()


@pytest.fixture()
def target(request, project_dir):
    with project_dir.as_cwd():
        log.info("Switched to directory {}".format(project_dir))
        from ymp.config import icfg
        icfg.init()
        for ds in icfg:
            yield request.param.format(ds)


@pytest.fixture()
def dump_logs(path=None):
    if path is None:
        path = py.Path.cwd()
    for f in path.iterdir():
        log.debug(f.name)
        if f.is_dir() and not f.name.startswith("."):
            dump_logs(path=f)
        elif f.name.endswith(".log"):
            log.error("Dumping logfile %s", f.name)
            with open(f) as fp:
                for line in fp:
                    log.error("%s: %s", f.stem, line.rstrip())
