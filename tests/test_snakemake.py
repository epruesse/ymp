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


def ymp_make(args):
    from click.testing import CliRunner
    from ymp.cli import make
    runner = CliRunner()
    result = runner.invoke(make, args)
    return result


@pytest.mark.parametrize("project", [("snakemake_circle")], indirect=True)
def test_snakemake_circle(project_dir):
    with project_dir.as_cwd():
        result = ymp_make(["test"])
        assert result.exit_code == 1
        assert "CircularReferenceException" in result.output


@pytest.mark.parametrize("project", [("snakemake_plain")], indirect=True)
def test_snakemake_plain(project_dir):
    with project_dir.as_cwd():
        result = ymp_make(["test"])
        assert result.exit_code == 0


@pytest.mark.parametrize("project", [("snakemake_function")], indirect=True)
def test_snakemake_function(project_dir):
    with project_dir.as_cwd():
        result = ymp_make(["test"])
        assert result.exit_code == 0
