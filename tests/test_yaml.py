import logging
import os

import pytest

from ymp import yaml

log = logging.getLogger(__name__)


def test_mixed_type(saved_tmpdir):
    with open(saved_tmpdir / "ymp.yml", "w") as fdes:
        fdes.write("data: string")
    with open(saved_tmpdir / "other.yml", "w") as fdes:
        fdes.write("data: [listofstring]")
    with pytest.raises(yaml.LayeredConfError) as excinfo:
        config = yaml.load([saved_tmpdir / "ymp.yml", saved_tmpdir / "other.yml"])
        config["data"]
    excinfo.value.show()


def test_mixed_type_tag_workdir(saved_tmpdir):
    with open(saved_tmpdir / "ymp.yml", "w") as fdes:
        fdes.write("data: string")
    with open(saved_tmpdir / "other.yml", "w") as fdes:
        fdes.write("data: !workdir string")
    config = yaml.load([saved_tmpdir / "ymp.yml", saved_tmpdir / "other.yml"])
    assert config.get_path("data") == "string"


def test_recusion_in_includes(saved_tmpdir):
    with open(saved_tmpdir / "ymp.yml", "w") as fdes:
        fdes.write("include: other.yaml")
    with open(saved_tmpdir / "other.yaml", "w") as fdes:
        fdes.write("include: ymp.yml")
    with pytest.raises(yaml.LayeredConfError) as excinfo:
        yaml.load([saved_tmpdir / "ymp.yml"])
    excinfo.value.show()


def test_missing_file(saved_tmpdir):
    with pytest.raises(yaml.LayeredConfError):
        yaml.load([saved_tmpdir / "missing.yaml"])
    with open(saved_tmpdir / "ymp.yml", "w") as fdes:
        fdes.write("include: missing.yaml")
    with pytest.raises(yaml.LayeredConfError) as excinfo:
        yaml.load([saved_tmpdir / "ymp.yml"])
    excinfo.value.show()


def test_toplevel_is_mapping(saved_tmpdir):
    ymp_yml = saved_tmpdir / "ymp.yml"
    with open(ymp_yml, "w") as fdes:
        fdes.write("- asd")
    with pytest.raises(yaml.LayeredConfError) as excinfo:
        yaml.load([ymp_yml])
    excinfo.value.show()


@pytest.mark.parametrize("project", ["recursive-include"], indirect=True)
def test_recursive_include(project_dir):
    config = yaml.load([
        project_dir / "defaults.yml",
        project_dir / "ymp.yml",
    ])

    assert config.defaults_loaded
    assert config.local_before_module_loaded
    assert config.module_pipeline2_loaded
    assert config.module_pipeline_loaded
    assert config.local_after_module_loaded
    assert config.main_loaded

    assert config.defaults_overridden
    assert config.local_before_module_overridden

    fnames = [os.path.basename(fname) for fname in reversed(config.get_files())]
    expected_fnames = [
        "defaults.yml",
        "local_before_module.yaml",
        "pipeline2.yaml",
        "pipeline.yaml",
        "local_after_module.yaml",
        "ymp.yml"
    ]
    assert fnames == expected_fnames


@pytest.mark.parametrize("project", ["recursive-include"], indirect=True)
def test_relative_path(project_dir):
    config = yaml.load([
        project_dir / "defaults.yml",
        project_dir / "ymp.yml",
    ])
    assert config.get_path("local_path") == "local_file"
    assert config.local_path_list.get_path(0) == "local_file_1"
    assert config.local_path_list.get_path(1) == "local_file_2"
    assert config.local_path_list.get_paths() == ["local_file_1", "local_file_2"]

    assert config.get_path("module_path") == "module/module_file"
    assert config.module_path_list.get_path(0) == "module/module_file_1"
    assert config.module_path_list.get_path(1) == "module/module_file_2"
    assert config.module_path_list.get_paths() == ["module/module_file_1", "module/module_file_2"]
    assert config.module_path_dict.get_paths() == {"path1": "module/module_file_3", "path2": "module/module_file_4"}

    assert config.get_path("absolute_path") == "/etc"

    assert config.get_path("path_overridden_by_main") == "overridden_path"
    assert config.get_path("path_overridden_by_module") == "module/overridden_path"
