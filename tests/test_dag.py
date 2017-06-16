import pytest
import py
import logging
import os

from collections import OrderedDict
class slice2OrderedDict(object):
    def __getitem__(self, keys):
        return OrderedDict([(slice.start, slice.stop) for slice in keys])
odict = slice2OrderedDict()


config_dirs = [
    'ibd'
]


# test subject : build target
targets = odict[
    ## fastq.rules
    'import':         'reports/{}_qc.html',
    'BBMap_ecco':     'reports/{}.ecco_qc.html',
    'BBDuk_trim':     'reports/{}.trimAQ10_qc.html',
    'BBDuk_rm_human': 'reports/{}.xhum_qc.html',
    'BB_dedupe':      'reports/{}.ddp_qc.html',
    'phyloFlash':     'reports/{}_heatmap.pdf',
    ## assembly.rules
    'assemble_separate_mh': 'reports/{}.by_ID.mhc.mq.html',
    'assemble_grouped_mh':  'reports/{}.by_SUBJECT.mhc.mq.html',
    'assemble_joined_mh':   'reports/{}.mhc.mq.html',
    'assemble_separate_sp': 'reports/{}.by_ID.sp.mq.html',
    'assemble_grouped_sp':  'reports/{}.by_SUBJECT.sp.mq.html',
    'assemble_joined_sp':   'reports/{}.sp.mq.html',
    ## mapping.rules
    'map_bbmap_separate':   '{}.by_ID.mhc.bbm/complete',
    'map_bbmap_grouped':   '{}.by_SUBJECT.mhc.bbm/complete',
    'map_bbmap_joined':   '{}.mhc.bbm/complete',
    'map_bowtie2_separate': '{}.by_ID.mhc.bt2/complete',
    'map_bowtie2_grouped': '{}.by_SUBJECT.mhc.bt2/complete',
    'map_bowtie2_joined': '{}.mhc.bt2/complete',
    ## blast.rules
    'blast_gene_find':   '{}.by_Subject.mhc.blast/psa.wcfR.csv',
    ## otu.rules
    'otu_table':         '{}.by_Subject.mhc.blast.otu/psa.wcfR.otu_table.csv'
]


@pytest.fixture(params=config_dirs)
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
    

@pytest.fixture(params=list(targets.values()), ids=list(targets.keys()))
def build_graph(request, project_dir):
    target = request.param
    with project_dir.as_cwd():
        from ymp.config import icfg
        icfg.init()
        for ds in icfg:
            G = make_graph(target.format(ds))
            R = make_graph(target.format(ds), rulegraph=True)
            yield (icfg[ds], G, R)


def test_graph_complete(build_graph):
    cfg, G, R = build_graph
    n_start_nodes = len(
        [1
         for node, degree in G.in_degree().items()
         if degree == 0])
    n_runs = len(cfg.runs)

    print("Testing start-nodes ({}) == runs ({})"
          "".format(n_start_nodes, n_runs))
    assert n_start_nodes == n_runs
