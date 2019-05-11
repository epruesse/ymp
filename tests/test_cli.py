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
    cfg.unload()

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


def test_env_prepare(invoker, demo_dir, mock_conda, mock_downloader):
    """Test passing through to snakemake prepare"""
    with open("ymp.yml", "a") as f:
        f.write("directories:\n conda_prefix: '.'")
    res = invoker.call("env", "list", "bbmap")
    lines = res.output.splitlines()
    col = lines[0].index("installed")
    assert lines[1][col:col+len("False")] == "False"
    invoker.initialized = False
    res = invoker.call("env", "prepare", "toy.trim_bbmap")

    res = invoker.call("env", "list", "bbmap")
    lines2 = res.output.splitlines()
    col = lines2[0].index("installed")
    assert lines2[1][col:col+len("True")] == "True", "\n".join(lines + lines2)

    conda_cmd = mock_conda.calls[-1]
    assert "conda create" in conda_cmd
    assert "/bbmap-" in conda_cmd


def test_env_install(invoker, demo_dir, mock_conda, mock_downloader):
    """Test installing environments"""
    with open("ymp.yml", "a") as f:
        f.write("directories:\n conda_prefix: '.'")

    # basic
    res = invoker.call("env", "install", "bbmap")
    assert "Creating 1 environments" in res.output
    assert "'bbmap'" in res.output
    assert "--prefix "+str(demo_dir) in mock_conda.calls[0]
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
    assert "--prefix "+str(demo_dir) in mock_conda.calls[1]
    assert len(mock_conda.calls) == 2

    # dynamic env
    res = invoker.call("env", "install", "sickle")
    assert "Creating 1 environments" in res.output
    assert "'sickle'" in res.output
    assert "--prefix "+str(demo_dir) in mock_conda.calls[2]


def test_env_update(invoker, demo_dir, mock_conda, mock_downloader):
    """Test updating environments"""
    with open("ymp.yml", "a") as f:
        f.write("directories:\n conda_prefix: '.'")
    # basic
    res = invoker.call("env", "update", "bbmap")
    assert "Updating 1 environments" in res.output
    assert "'bbmap'" in res.output
    assert "conda create" in mock_conda.calls[0]
    assert "conda env update" in mock_conda.calls[1]


def test_env_export(invoker, demo_dir, mock_conda, mock_downloader):
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


def test_env_clean(invoker, demo_dir, mock_conda):
    """Test cleaning environments"""
    with open("ymp.yml", "a") as f:
        f.write("directories:\n conda_prefix: '.'")


def test_env_activate(invoker, demo_dir, mock_conda, mock_downloader):
    """Test activating an environment"""
    with open("ymp.yml", "a") as f:
        f.write("directories:\n conda_prefix: '.'")
    res = invoker.call("env", "activate", "bbmap")
    assert str(demo_dir) in res.output


def test_env_run(invoker, demo_dir, mock_conda, mock_downloader, capfd):
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


@pytest.mark.parametrize(
    "words,result",
    [
        ["ymp make", [
            "toy", "toy.", "mpic", "mpic.", 0
        ]],
        ["ymp make t", [
            "toy", "toy.", 0
        ]],
        ["ymp make toy.", [
            "toy.assemble_", "toy.trim_"
        ]],
        ["ymp make toy.assemble_", [
            "toy.assemble_megahit",
            "toy.assemble_megahit."
         ]],
        ["ymp make toy.assemble_megahit.", [
            "toy.assemble_megahit.trim_",
            "toy.assemble_megahit.map_"
        ]],
        ["ymp make toy.assemble_megahit.map_", [
            "toy.assemble_megahit.map_bbmap",
        ]],
        ["ymp make toy.map_bowtie2.", [
            0
        ]],
        ["ymp make toy.group_", [
            "toy.group_name", "toy.group_Subject",
            "toy.group_name.", "toy.group_Subject.",
            10
        ]],
    ]
)
def test_completion(invoker, demo_dir, capfd, envvar, words, result):
    import subprocess as sp
    envvar('YMP_DEBUG_EXPAND', 'stderr')
    envvar('_YMP_COMPLETE', 'complete-bash')
    envvar('COMP_CWORD', '2')
    envvar('COMP_WORDS', words)
    sp.run(["python", "-m", "ymp"])
    cap = capfd.readouterr()
    expanded = set(cap.out.split())

    for val in result:
        if isinstance(val, str):
            expanded.remove(val)
        else:
            assert len(expanded) == val
