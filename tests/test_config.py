import pytest


@pytest.mark.parametrize("project",
                         ['ibd', 'toy', 'mpic', 'complex_data'],
                         indirect=True)
def test_config(project_dir):
    with project_dir.as_cwd():
        from ymp.config import icfg
        icfg.init()


@pytest.mark.parametrize("project, fq_names",
                         [('ibd', (738, 738, 0, 369, 369, 369)),
                          ('toy', (2, 2, 0, 1, 1, 1)),
                          ('mpic', (4, 0, 4, 0, 0, 4))],
                         indirect=['project'])
def test_fqfiles(project_dir, fq_names):
    with project_dir.as_cwd():
        from ymp import config as c
        c.icfg.init(force=True)
        for ds in c.icfg.datasets:
            assert len(c.icfg[ds].fq_names) == fq_names[0]
            assert len(c.icfg[ds].pe_fq_names) == fq_names[1]
            assert len(c.icfg[ds].se_fq_names) == fq_names[2]
            assert len(c.icfg[ds].fwd_pe_fq_names) == fq_names[3]
            assert len(c.icfg[ds].rev_pe_fq_names) == fq_names[4]
            assert len(c.icfg[ds].fwd_fq_names) == fq_names[5]
