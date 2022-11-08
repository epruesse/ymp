"""
Testing extensions to snakemake

TODO:
 - recursive parameter expansion
   X detect loop
   - numbered parameters
   - named parameters
   - named parameter sets
   - functions
 - ymp config expansion
   - general
   - localized
   - grouping
 - defaults
"""

import logging
import sys

import pytest

import ymp
from ymp.snakemake import check_snakemake
from ymp.exceptions import YmpException
from packaging import version


log = logging.getLogger(__name__)


def test_snakemake_version():
    assert check_snakemake(), "Snakemake version unsupported (too old)"
    minvers = version.parse(ymp.snakemake_minimum_version)
    testvers = version.parse(ymp.snakemake_tested_version)
    assert (
        minvers <= testvers
    ), "Minimum snakemake version must not be larger than tested version"


def test_snakemake_version_below_min_raises(monkeypatch):
    with monkeypatch.context() as m:
        m.setattr("ymp.snakemake_minimum_version", "99!1")
        m.setattr("ymp.snakemake.check_snakemake.result", None)
        with pytest.raises(YmpException):
            check_snakemake()
    assert check_snakemake(), "cached value not reset?"

@pytest.mark.xfail(
    sys.platform == "darwin",
    reason="unclear with this is failing on osx, likely the test"
)
def test_snakemake_version_above_tested_warns(monkeypatch, caplog):
    with monkeypatch.context() as m:
        m.setattr("ymp.snakemake_tested_version", "0")
        m.setattr("ymp.snakemake.check_snakemake.result", None)
        check_snakemake()
        msg_count = sum(
            "newer than the latest version" in rec.message for rec in caplog.records
        )
        assert msg_count == 1
    assert check_snakemake(), "cached value not reset?"


def test_snakemake_version_above_tested_warns_invoked(
    invoker, demo_dir, monkeypatch, caplog
):
    with monkeypatch.context() as m:
        m.setattr("ymp.snakemake_tested_version", "0")
        m.setattr("ymp.snakemake.check_snakemake.result", None)
        invoker.call("make", "-n", "toy")
        msg_count = sum(
            "newer than the latest version" in rec.message for rec in caplog.records
        )
        assert msg_count == 1


def test_snakemake_version_above_tested_quiet_with_q(
    invoker, demo_dir, monkeypatch, caplog
):
    with monkeypatch.context() as m:
        m.setattr("ymp.snakemake_tested_version", "0")
        m.setattr("ymp.snakemake.check_snakemake.result", None)
        invoker.call("make", "-nq", "toy")
        msg_count = sum(
            "newer than the latest version" in rec.message for rec in caplog.records
        )
        assert msg_count == 0


@pytest.mark.parametrize("project", ["snakemake_circle"], indirect=True)
def test_snakemake_failure(project_dir, invoker):
    "These are expected to fail"
    res = invoker.call_raises("make", "test")
    msg = str(res.output)
    assert "Circular reference in" in msg


@pytest.mark.parametrize(
    "project", ["snakemake_plain", "snakemake_function"], indirect=True
)
def test_snakemake(project_dir, invoker):
    "These should work"
    res = invoker.call("make", "test")
    assert res.exit_code == 0
