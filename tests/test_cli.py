import logging
import os

import click
import pytest

import ymp
cfg = ymp.get_config()

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


def test_submit_no_profile(invoker):
    "Running submit without profile should raise UsageError"
    with pytest.raises(click.UsageError):
        invoker.call("submit")


def test_submit_profile_cfg(invoker, saved_tmpdir):
    "Test profile set from config"
    with open("ymp.yml", "w") as ymp_yml:
        ymp_yml.write("cluster:\n  profile: dummy")
    invoker.call("submit", cfg.dir.reports)
    assert os.path.isdir(cfg.dir.reports)


# - don't test profiles that have no command set on them (default profile)
# - sort profiles so we can test in parallel reliably
profiles = sorted((name, profile)
                  for name, profile in cfg.cluster.profiles.items()
                  if profile.get('command'))


@pytest.mark.parametrize(
    "mock_cmd,prof_name,prof_cmd",
    [((profile.command.split()[0], '#!/bin/bash\nexec "$@"\n'),
      name, profile.command)
     for name, profile in profiles],
    ids=[name for name, profile in profiles],
    indirect=['mock_cmd'])
def test_submit_profiles(invoker, mock_cmd, prof_name, prof_cmd):
    invoker.call("submit",
                 "--profile", prof_name,
                 "--command", prof_cmd,
                 cfg.dir.reports)
    assert os.path.isdir(cfg.dir.reports)


def test_show(invoker, saved_tmpdir):
    "Test parts of show"
    with open("ymp.yml", "w") as cfg:
        cfg.write("conda:\n  testme: [X1,X2]")

    invoker.call("show")
    res = invoker.call("show", "pairnames")
    assert res.output.strip() == '- R1\n- R2'
    res = invoker.call("show", "pairnames[1]")
    assert res.output.strip() == 'R2'
    res = invoker.call("show", "cluster.profiles.default.drmaa")
    assert res.output.strip() == 'False'

    res = invoker.call("show", "conda.testme")
    assert res.output.strip() == '- X1\n- X2'


def test_stage_list(invoker):
    "List all stages"
    res = invoker.call("stage", "list")
    assert "\ncheck " in res.output

    with pytest.raises(click.UsageError):
        res = invoker.call("stage", "list", "-s", "-l")

    res = invoker.call("stage", "list", "does_not_exist")
    assert res.output == ""

    res = invoker.call("stage", "list", "ch?ck", "-s")
    assert res.output.strip() == "check"

    res = invoker.call("stage", "list", "ch?ck", "-l")
    assert res.output.startswith("check")
    assert res.output.count("\n") > 3

    res = invoker.call("stage", "list", "ch?ck", "-c")
    assert res.output.startswith("check")
    assert "test.rules:" in res.output
    assert res.output.count("\n") == 3


def test_func_get_envs():
    "Test env cli helper function get_envs"
    from ymp.cli.env import get_envs

    envs = get_envs()
    log.debug("envs found: %s", envs)
    assert 'bbmap' in envs
    assert 'bmtagger' in envs

    envs = get_envs('bbmap')
    assert len(envs) == 1

    envs = get_envs(['bbmap', 'bmtagger'])
    assert len(envs) == 2

    envs = get_envs(['bb?ap', 'bmtagger*'])
    assert len(envs) == 2


def test_env_list(invoker):
    """Test listing environments

    - w/o args
    - reverse sorted
    - sorted by hash
    """
    res = invoker.call("env", "list")
    lines = res.output.splitlines()
    assert len(lines) > 2
    assert lines[0].startswith("name"), "first row should start with name"
    assert all(lines[i].upper() <= lines[i+1].upper()
               for i in range(2, len(lines)-1)), \
        f"output should be sorted: {lines}"

    res = invoker.call("env", "list", "-r")
    lines = res.output.splitlines()
    assert all(lines[i].upper() >= lines[i+1].upper()
               for i in range(1, len(lines)-1)), \
        f"output should be sorted reverse:\n{lines}"

    res = invoker.call("env", "list", "-s", "hash")
    lines = res.output.splitlines()
    hash_col = lines[0].split().index("hash")
    hashes = [line.split()[hash_col] for line in lines]
    assert all(hashes[i] <= hashes[i+1]
               for i in range(1, len(lines)-1)), \
        f"output should be sorted by hash:\n{hashes[1:]}"


def test_env_prepare(invoker, project_dir, mock_conda):
    """Test passing through to snakemake prepare"""
    with open("ymp.yml", "a") as f:
        f.write("directories:\n conda_prefix: '.'")
    res = invoker.call("env", "list", "bbmap")
    lines = res.output.splitlines()
    col = lines[0].index("installed")
    assert lines[1][col:col+len("False")] == "False"
    invoker.initialized = False
    res = invoker.call("env", "prepare",
                       "--conda-prefix=.",
                       "toy.trim_bbmap/all")

    res = invoker.call("env", "list", "bbmap")
    lines = res.output.splitlines()
    col = lines[0].index("installed")
    assert lines[1][col:col+len("True")] == "True"


def test_env_install(invoker, project_dir, mock_conda):
    """Test installing environments"""
    with open("ymp.yml", "a") as f:
        f.write("directories:\n conda_prefix: '.'")

    # basic
    res = invoker.call("env", "install", "bbmap")
    assert "Creating 1 environments" in res.output
    assert "'bbmap'" in res.output
    assert "--prefix "+str(project_dir) in mock_conda.calls[0]
    assert len(mock_conda.calls) == 1

    # no double install
    res = invoker.call("env", "install", "bbmap")
    assert len(mock_conda.calls) == 1

    # remove bbmap env
    res = invoker.call("env", "remove", "bbmap")

    # multiple, globbing
    res = invoker.call("env", "install", "bb?ap", "bbma*")
    assert "Creating 1 environments" in res.output
    assert "'bbmap'" in res.output
    assert "--prefix "+str(project_dir) in mock_conda.calls[1]
    assert len(mock_conda.calls) == 2

    # dynamic env
    res = invoker.call("env", "install", "sickle")
    assert "Creating 1 environments" in res.output
    assert "'sickle'" in res.output
    assert "--prefix "+str(project_dir) in mock_conda.calls[2]


def test_env_update(invoker, project_dir, mock_conda):
    """Test updating environments"""
    with open("ymp.yml", "a") as f:
        f.write("directories:\n conda_prefix: '.'")
    # basic
    res = invoker.call("env", "update", "bbmap")
    assert "Updating 1 environments" in res.output
    assert "'bbmap'" in res.output
    assert "conda create" in mock_conda.calls[0]
    assert "conda env update" in mock_conda.calls[1]


def test_env_export(invoker, project_dir, mock_conda):
    """Test exporting environments"""
    # install envs locally
    with open("ymp.yml", "a") as f:
        f.write("directories:\n conda_prefix: '.'")

    # skip and create are mutually exclusive
    with pytest.raises(click.UsageError) as exc:
        invoker.call("env", "export", "-sc", "bbmap")
    assert exc.match("mutually exclusive")

    # fail if trying to export uninstalled env
    with pytest.raises(click.UsageError) as exc:
        res = invoker.call("env", "export", "bbmap")
    assert exc.match("uninstalled")

    # exporting nothing
    res = invoker.call("env", "export", "-qs", "bbmap")
    assert res.output == ""

    # creating bbmap.yml
    res = invoker.call("env", "export", "-d", ".", "bbmap", "-c")
    assert "Exporting" in res.output

    # fail, file exists
    with pytest.raises(click.UsageError) as exc:
        res = invoker.call("env", "export", "-d", ".", "bbmap")
    assert exc.match("exists")

    # allow overwrite
    res = invoker.call("env", "export", "-d", ".", "bbmap", "-f")
    assert "Exporting" in res.output

    # try txt format and export to file
    res = invoker.call("env", "export", "-d", "bbmap.txt", "bbmap")
    assert "conda list" in mock_conda.calls[-1]

    # export multiple, fail
    with pytest.raises(click.UsageError) as exc:
        res = invoker.call("env", "export", "-cd", ".", "bbmap", "sambamba")
    assert exc.match("exists")

    # export multiple
    res = invoker.call("env", "export", "-fcd", ".", "bbmap", "sambamba")
    assert "Exporting 2 " in res.output

    # export multiple to stdout
    res = invoker.call("env", "export", "-q", "bbmap", "sambamba")
    names = [line[6:] for line in res.output.splitlines()
             if line.startswith("name: ")]
    assert sorted(names) == ["bbmap", "sambamba"]

    # export no matching patterns
    res = invoker.call("env", "export", "does_not_match_anything")
    assert "Nothing to export" in res.output

    # export everything installed
    res = invoker.call("env", "export", "-s")
    names = [line[6:] for line in res.output.splitlines()
             if line.startswith("name: ")]
    assert sorted(names) == ["bbmap", "sambamba"]


def test_env_clean(invoker, project_dir, mock_conda):
    """Test cleaning environments"""
    with open("ymp.yml", "a") as f:
        f.write("directories:\n conda_prefix: '.'")


def test_env_activate(invoker, project_dir, mock_conda):
    """Test activating an environment"""
    with open("ymp.yml", "a") as f:
        f.write("directories:\n conda_prefix: '.'")
    res = invoker.call("env", "activate", "bbmap")
    assert str(project_dir) in res.output


def test_env_run(invoker, project_dir, mock_conda, capfd):
    with open("ymp.yml", "a") as f:
        f.write("directories:\n conda_prefix: '.'")

    with pytest.raises(click.UsageError) as exc:
        res = invoker.call("env", "run", "bbmapx", "bbmap.sh")
    assert exc.value.message == "Environment bbmapx unknown"

    with pytest.raises(click.UsageError) as exc:
        res = invoker.call("env", "run", "*", "bbmap.sh")
    assert exc.value.message.startswith("Multiple environments match")

    res = invoker.call("env", "run", "bbmap", "true")
    assert res.exit_code == 0
    cap = capfd.readouterr()
    assert "Not a conda environment" in cap.err
