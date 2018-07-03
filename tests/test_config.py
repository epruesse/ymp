import pytest

import ymp


@pytest.mark.parametrize("project",
                         ['ibd', 'toy', 'mpic', 'complex_data'],
                         indirect=True)
def test_config(project_dir):
    with project_dir.as_cwd():
        ymp.get_config().unload()
        cfg = ymp.get_config()


@pytest.mark.parametrize("project, fq_names",
                         [('ibd', (738, 738, 0, 369, 369, 369)),
                          ('toy', (2, 2, 0, 1, 1, 1)),
                          ('mpic', (4, 0, 4, 0, 0, 4))],
                         indirect=['project'])
def test_fqfiles(project_dir, fq_names):
    with project_dir.as_cwd():
        ymp.get_config().unload()
        cfg = ymp.get_config()
        for ds in cfg.projects.values():
            assert len(ds.fq_names) == fq_names[0]
            assert len(ds.pe_fq_names) == fq_names[1]
            assert len(ds.se_fq_names) == fq_names[2]
            assert len(ds.fwd_pe_fq_names) == fq_names[3]
            assert len(ds.rev_pe_fq_names) == fq_names[4]
            assert len(ds.fwd_fq_names) == fq_names[5]
