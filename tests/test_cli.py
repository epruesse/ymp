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
    assert lines[0].startswith("label"), "first row should start with name"
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
        f.write("directories:\n conda_prefix: '.'\nconda:\n frontend: conda\n")

    # basic
    res = invoker.call("env", "install", "bbmap")
    assert "Creating 1 environments" in res.output
    assert "'bbmap'" in res.output
    assert "--prefix "+str(demo_dir) in mock_conda.calls[-1]
    n_calls = len(mock_conda.calls)

    # no double install
    res = invoker.call("env", "install", "bbmap")
    assert len(mock_conda.calls) == n_calls

    # remove bbmap env
    res = invoker.call("env", "remove", "bbmap")

    # multiple, globbing
    res = invoker.call("env", "install", "bb?ap", "bbma*")
    assert "Creating 1 environments" in res.output
    assert "'bbmap'" in res.output
    assert "--prefix "+str(demo_dir) in mock_conda.calls[-1]
    assert len(mock_conda.calls) == n_calls + 1

    # dynamic env
    res = invoker.call("env", "install", "sickle")
    assert "Creating 1 environments" in res.output
    assert "'sickle'" in res.output
    assert "--prefix "+str(demo_dir) in mock_conda.calls[-1]


def test_env_update(invoker, demo_dir, mock_conda, mock_downloader):
    """Test updating environments"""
    with open("ymp.yml", "a") as f:
        f.write("directories:\n conda_prefix: '.'\nconda:\n frontend: conda\n")
    # basic
    res = invoker.call("env", "update", "bbmap")
    assert "Updating 1 environments" in res.output
    assert "'bbmap'" in res.output
    assert "conda create" in mock_conda.calls[-2]
    assert "conda env update" in mock_conda.calls[-1]


def test_env_export(invoker, demo_dir, mock_conda, mock_downloader):
    """Test exporting environments"""
    # install envs locally
    with open("ymp.yml", "a") as f:
        f.write("directories:\n conda_prefix: '.'\nconda:\n frontend: conda\n")

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
        f.write("directories:\n conda_prefix: '.'\nconda:\n frontend: conda\n")


def test_env_activate(invoker, demo_dir, mock_conda, mock_downloader):
    """Test activating an environment"""
    with open("ymp.yml", "a") as f:
        f.write("directories:\n conda_prefix: '.'\nconda:\n frontend: conda\n")
    res = invoker.call("env", "activate", "bbmap")
    assert str(demo_dir) in res.output


def test_env_run(invoker, demo_dir, mock_conda, mock_downloader, capfd):
    with open("ymp.yml", "a") as f:
        f.write("directories:\n conda_prefix: '.'\nconda:\n frontend: conda\n")

    with pytest.raises(click.UsageError) as exc:
        res = invoker.call("env", "run", "bbmapx", "bbmap.sh")
    assert exc.value.message == "Environment bbmapx unknown"

    with pytest.raises(click.UsageError) as exc:
        res = invoker.call("env", "run", "*", "bbmap.sh")
    assert exc.value.message.startswith("Multiple environments match")

    res = invoker.call("env", "run", "bbmap", "true")
    assert res.exit_code == 0
    cap = capfd.readouterr()
    assert (
        "bin/activate: No such file or directory" in cap.err
        or "Not a conda environment:" in cap.err
    )


@pytest.mark.parametrize(
    "comp_words,exp_len,exp_res",
    [
        ["ymp make", 6, {
            "toy", "toy.", "mpic", "mpic."
        }],
        ["ymp make t", 2, {
            "toy", "toy."
        }],
        ["ymp make toy.", -1, {
            "toy.assemble_", "toy.trim_"
        }],
        ["ymp make toy.assemble_", -1, {
            "toy.assemble_megahit",
            "toy.assemble_megahit."
        }],
        ["ymp make toy.assemble_megahit.", -1, {
            "toy.assemble_megahit.trim_",
            "toy.assemble_megahit.ref_"
        }],
        ["ymp make toy.assemble_megahit.map_", -1, {
            "toy.assemble_megahit.map_bbmap",
        }],
        ["ymp make toy.map_bowtie2.", 0, set()],
        ["ymp make toy.group_", 16, {
            "toy.group_name", "toy.group_Subject",
            "toy.group_name.", "toy.group_Subject.",
            "toy.group_ALL.", "toy.group_ALL",
        }],
    ]
)
def test_completion(
        # fixtures:
        invoker, demo_dir, capfd, envvar,
        # parameters:
        comp_words,  # command line prefix to expand
        exp_len,     # expected number of result options (or -1)
        exp_res      # (subset of) expected result options
):
    """This tests click completion by launching an external python
    process and checking the output it would return to click's bash
    code. If things change within click, this code will have to change
    too.

    """

    import subprocess as sp
    # Set an environment variable that will make expansion code blab
    # to stderr for debugging:
    envvar('YMP_DEBUG_EXPAND', 'stderr')
    # Set the trigger variable that will initiate bash completion by
    # click:
    envvar('_YMP_COMPLETE', 'bash_complete')
    # Pass the variables bash would set to request completion of the
    # 2nd word after the command name, which in this case is the stage
    # stack name.
    envvar('COMP_CWORD', '2')
    envvar('COMP_WORDS', comp_words)
    # Run and capture:
    sp.run(["python", "-m", "ymp"])
    cap = capfd.readouterr()
    # Click sends one line per expansion in form $type,$value. If the
    # type is "plain", the value is added as expansion option. If the
    # type is dir or file, directory or filename expansion is enabled
    # in case no values match, and $value is ignored. We wrap types
    # other than plain in double underscore and otherwise keep the
    # value to compare to expected test results.
    result = set()
    for line in cap.out.splitlines():
        if exp_len == 0 and not line:
            continue  # empty line ok for empty result
        assert line.count(",") == 1, f"wrong field count in {line}"
        typ, val = line.split(",")
        if typ == "plain":
            result.add(val)
        else:
            result.add(f"__{typ}__")

    assert exp_len == -1 or len(result) == exp_len, \
        f"Expected {exp_len} results for '{comp_words}' but got" \
        f" {len(result)}:\n" \
        f"{result}"

    assert exp_res.issubset(result), \
        f"Completion for '{comp_words}' is missing: {exp_res - result}"
