import pytest
import py
import logging
import os

targets = [
    # test import
    'reports/{}_qc.html',
    # test rule bb_ecco
    'reports/{}.ecco_qc.html',
    # test rule trim_bbduk_adapter
    'reports/{}.trimAQ10_qc.html',
    # test rule bbmap_rmhuman
    'reports/{}.xhum_qc.html',
    # test rule bbmap_dedupe
    'reports/{}.ddp_qc.html',
    # test rule phyloFlash and rule phyloFlash_heatmap
    'reports/{}_heatmap.pdf',
    '{}.ecco.trimAQ10.xhum.by_Subject.mhc.blast.otu/psa.wcfR.otu_table.csv'
]

@pytest.fixture(params=['ibd'])
def project_dir(request, tmpdir):
    data_dir = py.path.local(__file__).dirpath('data', request.param)
    data_dir.copy(tmpdir)
    yield tmpdir
    tmpdir.remove()


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
    

@pytest.mark.parametrize("target", targets)
def test_graph_complete(project_dir, target):
    with project_dir.as_cwd():
        from ymp.config import icfg
        icfg.init()
        for ds in icfg:
            G = make_graph(target.format(ds))
            n_start_nodes = len(
                [1
                 for node, degree in G.in_degree().items()
                 if degree == 0])
            assert n_start_nodes == len(icfg[ds].runs)
