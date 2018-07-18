import logging

import pytest

import ymp

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


@pytest.mark.parametrize("project", ["complex_data"], indirect=True)
def test_load_data(project_dir, saved_cwd):
    ymp.get_config().unload()
    cfg = ymp.get_config()
    assert cfg.projects.test.run_data.to_dict() == \
        cfg.projects.compare.run_data.to_dict()
