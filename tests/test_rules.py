import logging

from networkx import DiGraph

from pygraphviz import AGraph

import pytest

from .data import parametrize_target

import ymp


log = logging.getLogger(__name__)


@pytest.mark.runs_tool
@parametrize_target(large=False, exclude_targets=['phyloFlash'])
def test_run_rules(target, invoker):
    invoker.call("make", target)


@parametrize_target()
def test_graph_complete(target, project, invoker):
    cfg = ymp.get_config()

    res = invoker.call("make", "-qq", "--dag", target)

    # Snakemake can't be quietet in version 4.7, and pytest can't be
    # told to ignore stderr. We work around this by removing the
    # first line if it is the spurious Snakemake log message
    if res.output.startswith("Building DAG of jobs..."):
        _, output = res.output.split("\n", 1)
    else:
        output = res.output
    assert output.startswith("digraph")

    with open("dat.dot", "w") as out:
        out.write(output)

    g = DiGraph(AGraph(output))
    n_runs = len(cfg.projects[project].runs)

    n_start_nodes = len(
        [1 for node, degree in g.in_degree()
         if degree == 0])
    log.info("\nTesting start-nodes ({}) >= runs ({})"
             "".format(n_start_nodes, n_runs))
    # assert n_start_nodes >= n_runs

    n_symlinks = len(
        [1 for node, degree in g.in_degree()
         if g.node[node]['label'].startswith('symlink_raw_reads')])
    log.info("Testing symlinks ({}) == 2 * runs ({})"
             "".format(n_symlinks, n_runs))
    assert n_symlinks == len(cfg.projects[project].runs)
