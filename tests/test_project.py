import logging

import pytest

import ymp
from ymp.projects import SQLiteProjectData, PandasProjectData

log = logging.getLogger(__name__)  # pylint: disable=invalid-name

def pickled_sqlite_pd(cfg):
    import pickle
    obj = SQLiteProjectData(cfg)
    pkl = pickle.dumps(obj)
    return pickle.loads(pkl)


@pytest.fixture(params=[PandasProjectData, SQLiteProjectData, pickled_sqlite_pd])
def project_data(request, project_dir, saved_cwd):
    ymp.get_config().unload()
    cfg = ymp.get_config()
    return [request.param(prj.cfg.data) for prj in cfg.projects.values()]


@pytest.mark.parametrize("project", ["complex_data"], indirect=True)
def test_load_data(project_data):
    print(project_data)
    assert project_data[0].dump() == project_data[1].dump()


@pytest.mark.parametrize("project", ["complex_data"], indirect=True)
def test_project_data(project_data):
    obj = project_data[0]
    cols = ['fruit', 'skin_color', 'meat_color', 'size', 'texture', 'taste',
            'weight', 'case']
    assert obj.columns() == cols
    assert obj.identifying_columns() == ['fruit']
    assert obj.duplicate_rows('fruit') == []
    assert obj.duplicate_rows('skin_color') == ['green', 'red', 'red', 'green']
    assert obj.string_columns() == cols
    for row, test in zip(
            obj.rows(['fruit']),
                     ['apple', 'orange', 'grape', 'banana', 'cherry', 'melon']
            ):
        assert row[1] == test
    assert obj.get('fruit', 'apple', 'skin_color') == ['green']
    assert obj.column('fruit') == ['apple', 'orange', 'grape', 'banana', 'cherry', 'melon']
    assert obj.groupby_dedup(cols[1:]) == cols[1:3]
