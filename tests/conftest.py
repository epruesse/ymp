import pytest
import py

config_dirs = [
    'ibd'
]

@pytest.fixture(params=config_dirs)
def project_dir(request, tmpdir):
    data_dir = py.path.local(__file__).dirpath('data', request.param)
    data_dir.copy(tmpdir)
    yield tmpdir
    tmpdir.remove()
