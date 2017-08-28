import logging

import pytest

from .data import targets

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
    from ymp.cmd import make as ymp_make
    from pygraphviz import AGraph
    from networkx import DiGraph
    runner = CliRunner()
    result = runner.invoke(ymp_make, [
        '--dag' if not rulegraph else '--rulegraph',
        target])
    assert result.exit_code == 0, result.output
    return DiGraph(AGraph(result.output))


@pytest.fixture(params=list(targets.values()), ids=list(targets.keys()))
def build_graph(request, project_dir):
    target = request.param
    with project_dir.as_cwd():
        from ymp.config import icfg
        icfg.init()
        for ds in icfg:
            g = make_graph(target.format(ds))
            # r = make_graph(target.format(ds), rulegraph=True)
            r = None
            yield (icfg[ds], g, r)


@pytest.mark.parametrize("project_dir", ['ibd'], indirect=True)
def test_graph_complete(build_graph):
    cfg, G, _ = build_graph
    n_runs = len(cfg.runs)

    n_start_nodes = len(
        [1 for node, degree in G.in_degree().items()
         if degree == 0])
    log.info("\nTesting start-nodes ({}) >= runs ({})"
             "".format(n_start_nodes, n_runs))
    assert n_start_nodes >= n_runs

    n_symlinks = len(
        [1 for node, degree in G.in_degree().items()
         if degree == 1 and
         G.node[node]['label'].startswith('symlink_raw_reads')])
    log.info("Testing symlinks ({}) == 2 * runs ({})"
             "".format(n_symlinks, n_runs))
    assert n_symlinks == 2 * n_runs
