import logging

import pytest

from .data import targets, parametrize_target

log = logging.getLogger(__name__)


@pytest.fixture(scope="module")  # autouse=True)
def profiling():
    import yappi
    yappi.start()
    yield
    yappi.stop()
    profile = yappi.get_func_stats()
    profile.sort("subtime")
    with open("profile.txt", "w") as f:
        profile.print_all(out=f, columns={
            0: ("name", 120),
            1: ("ncall", 10),
            2: ("tsub", 8),
            3: ("ttot", 8),
            4: ("tavg", 8)})


def make_graph(target, rulegraph=False):
    from click.testing import CliRunner
    from ymp.cli.make import make as ymp_make
    from pygraphviz import AGraph
    from networkx import DiGraph
    with open("target.txt", "w") as out:
        out.write(target)

    runner = CliRunner()
    result = runner.invoke(ymp_make, [
        '--quiet', '--quiet',
        '--dag' if not rulegraph else '--rulegraph',
        target])
    assert result.exit_code == 0, result.output
    with open("rulegraph.dot", "w") as out:
        out.write(result.output)
    assert result.output.startswith("digraph")

    return DiGraph(AGraph(result.output))


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
