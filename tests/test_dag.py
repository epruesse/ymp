import pytest
from ymp.common import odict
import yappi

@pytest.fixture(scope="module") # autouse=True)
def profiling():
    yappi.start()
    yield
    yappi.stop()
    profile = yappi.get_func_stats()
    profile.sort("subtime")
    with open("profile.txt", "w") as f:
        profile.print_all(out=f, columns = {
            0:("name",120),
            1:("ncall", 10),
            2:("tsub", 8),
            3: ("ttot", 8),
            4:("tavg",8)})


# test subject : build target
targets = odict[
    ## fastq.rules
    'import':         'reports/{}_qc.html',
    'BBMap_ecco':     'reports/{}.ecco_qc.html',
    'BBDuk_trim':     'reports/{}.trimAQ10_qc.html',
    'BBDuk_rm_human': 'reports/{}.xhum_qc.html',
    'BB_dedupe':      'reports/{}.ddp_qc.html',
    'phyloFlash':     'reports/{}_heatmap.pdf',
    'trimmomaticT32': 'reports/{}.trimmomaticT32_qc.html',
    'sickle':         'reports/{}.sickle_qc.html',
    'sickleQ10':      'reports/{}.sickleQ10_qc.html',
    'sickleL10':      'reports/{}.sickleL10_qc.html',
    'sickleQ10L10':   'reports/{}.sickleQ10L10_qc.html',
    'bmtaggerhs37':   'reports/{}.bmtaggerhs37_qc.html',
    ## assembly.rules
    'assemble_separate_mh': 'reports/{}.by_ID.mhc.mq.html',
    'assemble_grouped_mh':  'reports/{}.by_Subject.mhc.mq.html',
    'assemble_joined_mh':   'reports/{}.mhc.mq.html',
    'assemble_separate_sp': 'reports/{}.by_ID.sp.mq.html',
    'assemble_grouped_sp':  'reports/{}.by_Subject.sp.mq.html',
    'assemble_joined_sp':   'reports/{}.sp.mq.html',
    ## mapping.rules
    'map_bbmap_separate':   '{}.by_ID.mhc.bbm/complete',
    'map_bbmap_grouped':   '{}.by_Subject.mhc.bbm/complete',
    'map_bbmap_joined':   '{}.mhc.bbm/complete',
    'map_bowtie2_separate': '{}.by_ID.mhc.bt2/complete',
    'map_bowtie2_grouped': '{}.by_Subject.mhc.bt2/complete',
    'map_bowtie2_joined': '{}.mhc.bt2/complete',
    ## blast.rules
    'blast_gene_find':   '{}.by_Subject.mhc.blast/psa.wcfR.csv',
    ## coverage rules
    'coverage':          '{}.by_Subject.mhc.bbm.cov/blast.psa.wcfR.csv',
    'coverage':          '{}.by_Subject.mhc.bt2.cov/blast.psa.wcfR.csv'
    ## otu.rules
#    'otu_table':         '{}.by_Subject.mhc.blast.otu/psa.wcfR.otu_table.csv'
]


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
    cfg, G, _ = build_graph
    n_runs = len(cfg.runs)

    n_start_nodes = len(
        [1
         for node, degree in G.in_degree().items()
         if degree == 0
        ])
    print("\nTesting start-nodes ({}) >= runs ({})"
          "".format(n_start_nodes, n_runs))
    assert n_start_nodes >= n_runs

    n_symlinks = len(
        [1
         for node, degree in G.in_degree().items()
         if degree == 1 and G.node[node]['label'].startswith('symlink_raw_reads')
        ])
    print("Testing symlinks ({}) == 2 * runs ({})"
          "".format(n_symlinks, n_runs))
    assert n_symlinks == 2*n_runs
