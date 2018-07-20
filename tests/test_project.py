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


@pytest.mark.parametrize("project", ["complex_data"], indirect=True)
def test_pandas_project_data(project_dir, saved_cwd):
    ymp.get_config().unload()
    cfg = ymp.get_config()
    obj = cfg.projects.compare.run_data
    cols = ['fruit', 'skin_color', 'meat_color', 'size', 'texture', 'taste',
            'weight', 'case']
    assert obj.columns() == cols
    assert obj.identifying_columns() == ['fruit']
    assert list(obj.to_dict().keys()) == cols
    assert obj.duplicate_rows('fruit') == []
    assert obj.duplicate_rows('skin_color') == ['green', 'red', 'red', 'green']
    assert obj.string_columns() == cols

