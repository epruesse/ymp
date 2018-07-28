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

import pytest

log = logging.getLogger(__name__)


@pytest.mark.parametrize("project", ["snakemake_circle"], indirect=True)
def test_snakemake_failure(project_dir, invoker):
    "These are expected to fail"
    res = invoker.call_raises("make", "test")
    msg = str(res.output)
    assert "Circular reference in" in msg


@pytest.mark.parametrize("project", ["snakemake_plain", "snakemake_function"],
                         indirect=True)
def test_snakemake(project_dir, invoker):
    "These should work"
    res = invoker.call("make", "test")
    assert res.exit_code == 0
