import logging

import py

import pytest

log = logging.getLogger(__name__)


@pytest.fixture(params=[])
def project_dir(request, tmpdir):
    data_dir = py.path.local(__file__).dirpath('data', request.param)
    data_dir.copy(tmpdir)
    log.info("Created project directory {}".format(tmpdir))
    yield tmpdir
    # for entry in tmpdir.visit():
    #    log.info(entry)
    tmpdir.remove()