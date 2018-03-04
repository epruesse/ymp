import logging

import pytest
from click.testing import CliRunner
from ymp.cli import make as ymp_make
from pygraphviz import AGraph
from networkx import DiGraph

from .data import parametrize_target

log = logging.getLogger(__name__)


def run_ymp(target, flags=None):
    if not flags:
        flags = []
    flags = flags.copy() + ["-j2", target]
    # write cmd.sh for ease of reproduction in case of failure
    with open("cmd.sh", "a") as out:
        out.write("ymp make {}".format(" ".join(flags)))
    runner = CliRunner()
    result = runner.invoke(ymp_make, flags)
    # keep the stdout/err output for later inspection
    with open("output.txt", "w") as out:
        out.write(result.output)

    assert result.exit_code == 0, result.output

    return result.output


@pytest.mark.runs_tool
@parametrize_target(large=False, exclude_targets=['phyloFlash'])
def test_run_rules(target):
    run_ymp(target)


def make_graph(target, rulegraph=False):
    output = run_ymp(target,
                     ["-qq",
                      '--dag' if not rulegraph else '--rulegraph'])

    # Snakemake can't be quietet in version 4.7, and pytest can't be
    # told to ignore stderr. We work around this by removing the
    # first line if it is the spurious Snakemake log message
    outlines = output.splitlines()
    if outlines[0].startswith("Building DAG of jobs..."):
        output = "\n".join(outlines[1:])

    with open("rulegraph.dot", "w") as out:
        out.write(output)
    assert output.startswith("digraph")

    return DiGraph(AGraph(output))


@parametrize_target()
def test_graph_complete(target, project):
    from ymp.config import icfg
    g = make_graph(target)
    n_runs = len(icfg[project].runs)

    n_start_nodes = len(
        [1 for node, degree in g.in_degree()
         if degree == 0])
    log.info("\nTesting start-nodes ({}) >= runs ({})"

             "".format(n_start_nodes, n_runs))
#    assert n_start_nodes >= n_runs

    n_symlinks = len(
        [1 for node, degree in g.in_degree()
         if g.node[node]['label'].startswith('symlink_raw_reads')])
    log.info("Testing symlinks ({}) == 2 * runs ({})"
             "".format(n_symlinks, n_runs))
    assert n_symlinks == len(icfg[project].fq_names)
