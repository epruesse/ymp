import pytest

@pytest.mark.parametrize("project_dir", ['ibd','complex_data'], indirect=True)
def test_config(project_dir):
    with project_dir.as_cwd():
        from ymp.config import icfg
        icfg.init()
