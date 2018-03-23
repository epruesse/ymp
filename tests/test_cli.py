import logging
import os

import click
import pytest

log = logging.getLogger(__name__)


def test_submit_no_profile(invoker):
    # this should raise a UsageError
    with pytest.raises(click.UsageError):
        invoker.call("submit")


def test_submit_profiles(invoker):
    # try all profiles, but override the cmd to echo
    from ymp.config import icfg
    for profile_name, profile in icfg.cluster.profiles.items():
        if not profile.get('command'):
            continue
        # patch the submit command relative to CWD:
        profile.command = os.sep.join([".", profile.command])
        cmd = profile.command.split()[0]
        with open(cmd, "w") as f:
            f.write('#!/bin/sh\necho "$@">tmpfile\nexec "$@"\n')
            os.chmod(cmd, 0o755)
        if os.path.isdir(icfg.dir.reports):
            os.rmdir(icfg.dir.reports)
        invoker.call("submit",
                     "-p",
                     "--profile", profile_name,
                     "--command", profile.command,
                     icfg.dir.reports)
        assert os.path.isdir(icfg.dir.reports)


def test_show(invoker, saved_tmpdir):
    invoker.call("show")
    res = invoker.call("show", "pairnames")
    assert res.output.strip() == '- R1\n- R2'
    res = invoker.call("show", "pairnames[1]")
    assert res.output.strip() == 'R2'
    res = invoker.call("show", "cluster.profiles.default.drmaa")
    assert res.output.strip() == 'False'

    with open("ymp.yml", "w") as cfg:
        cfg.write("conda:\n  testme: [X1,X2]")
    res = invoker.call("show", "conda.testme")
    assert res.output.strip() == '- X1\n- X2'


def test_stage_list(invoker):
    res = invoker.call("stage", "list")
    assert "\ncheck " in res.output


def test_env_list(invoker):
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
    res = invoker.call("env", "prepare",
                       "--conda-prefix=.",
                       "toy.trim_bbmap/all")
    assert "bbmap.yml created" in res.output


def test_env_install(invoker, project_dir, mock_conda):
    with open("ymp.yml", "a") as f:
        f.write("directories:\n conda_prefix: '.'")

    # basic
    res = invoker.call("env", "install", "bbmap")
    assert "Creating 1 environments" in res.output
    assert "'bbmap'" in res.output
    assert "bbmap" in mock_conda.calls[0]
    assert "conda env create" in mock_conda.calls[0]

    # no double install
    res = invoker.call("env", "install", "bbmap")
    assert len(mock_conda.calls) == 1

    # remove bbmap env
    res = invoker.call("env", "clean", "bbmap")

    # multiple, globbing
    res = invoker.call("env", "install", "bb?ap", "bbma*")
    assert "Creating 1 environments" in res.output
    assert "'bbmap'" in res.output
    assert "bbmap" in mock_conda.calls[1]
    assert "conda env create" in mock_conda.calls[1]

    # dynamic env
    res = invoker.call("env", "install", "sickle")
    assert "Creating 1 environments" in res.output
    assert "'sickle'" in res.output
    assert "sickle" in mock_conda.calls[2]
    assert "conda env create" in mock_conda.calls[2]


def test_env_update(invoker, project_dir, mock_conda):
    with open("ymp.yml", "a") as f:
        f.write("directories:\n conda_prefix: '.'")
    # basic
    res = invoker.call("env", "update", "bbmap")
    assert "Updating 1 environments" in res.output
    assert "'bbmap'" in res.output
    assert "bbmap" in mock_conda.calls[0]
    assert "conda env create" in mock_conda.calls[0]
    assert "bbmap" in mock_conda.calls[1]
    assert "conda env update" in mock_conda.calls[1]


def test_env_clean(invoker, project_dir, mock_conda):
    with open("ymp.yml", "a") as f:
        f.write("directories:\n conda_prefix: '.'")


def test_env_activate(invoker, project_dir, mock_conda):
    with open("ymp.yml", "a") as f:
        f.write("directories:\n conda_prefix: '.'")
    res = invoker.call("env", "activate", "bbmap")
