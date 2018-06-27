from ymp.common import odict, ensure_list
from collections import OrderedDict

import pytest

data_types = ['any', 'metagenome', 'amplicon']

dataset_map = {
    'any': ['toy'], # 'mpic'],
    'metagenome': ['toy'],
    'large': ['ibd'],
    'amplicon': [] #'mpic']
}

target_map = {
    'any': odict[
        # fastq.rules
        'import':            '{}/all',
        'correct_bbmap':     '{}.correct_bbmap/all',
        'trim_bbmap':        '{}.trim_bbmapAQ10/all',
        'trim_sickle':       '{}.trim_sickle/all',
        'trim_sickleQ10':    '{}.trim_sickleQ10/all',
        'trim_sickleL10':    '{}.trim_sickleL10/all',
        'trim_sickleQ10L10': '{}.trim_sickleQ10L10/all',
        'time_trimmomaticT32': '{}.trim_trimmomaticT32/all',
        'filter_bbmap':      '{}.ref_phiX.index_bbmap.remove_bbmap/all',
        'filter_bmtagger':   '{}.ref_phiX.index_bmtagger.filter_bmtagger/all',
        'dedup_bbmap':       '{}.dedup_bbmap/all',
        'rm_bmtagger':       '{}.ref_phiX.index_bmtagger.remove_bmtagger/all',
        # fails due to bugs in phyloFlash with too few organisms
        #'phyloFlash':     'reports/{}.phyloFlash.pdf',
        'fastqc':            '{}.qc_fastqc/all',
        #'multiqc':           '{}.qc_fastqc.qc_multiqc/all',
    ],
    'metagenome': odict[
        # assembly.rules
        'assemble_separate_mh': '{}.assemble_megahit/all',
        'assemble_grouped_mh':  '{}.group_Subject.assemble_megahit/all',
        'assemble_joined_mh':   '{}.group_ALL.assemble_megahit/all',
        'assemble_separate_sp': '{}.assemble_metaspades/all',
        'assemble_grouped_sp':  '{}.group_Subject.assemble_metaspades/all',
        'assemble_joined_sp':   '{}.group_ALL.assemble_metaspades/all',
        # race condition in automatic db download in metaquast makes
        # running this on CI impossible at the moment
        #'metaquast_mh':         'reports/{}.assemble_megahit.mq.html',
        #'metaquast_sp':         'reports/{}.sp.mq.html',
        # mapping.rules
        'map_bbmap_separate':   '{}.assemble_megahit.map_bbmap/all',
        'map_bbmap_grouped':   '{}.group_Subject.assemble_megahit.map_bbmap/all',
        'map_bbmap_joined':   '{}.group_ALL.assemble_megahit.map_bbmap/all',
        'map_bowtie2_separate': '{}.assemble_megahit.index_bowtie2.map_bowtie2/all',
        'map_bowtie2_grouped': '{}.group_Subject.assemble_megahit.index_bowtie2.map_bowtie2/all',
        'map_bowtie2_joined': '{}.group_ALL.assemble_megahit.index_bowtie2.map_bowtie2/all',
        # mapping vs reference
        'map_bbmap_reference': '{}.ref_genome.map_bbmap/all',
        'map_bowtie2_reference': '{}.ref_genome.index_bowtie2.map_bowtie2/all',
        # community profile
        #'profile_metaphlan2': '{}.metaphlan2/all',
        # functional profile
        # broken on CI, probably due to memory or time limits
        # 'profile_humann2': '{}.humann2/all',
    ],
    'amplicon': odict[
        'bbmap_primer': '{}.primermatch_bbmap/all'
    ]
}


targets = OrderedDict()
for target_type in target_map:
    targets.update(target_map[target_type])


def get_targets(large=True, exclude_targets=None):
    target_dir_pairs = (
        pytest.param(dataset, target_map[dtype][target], dataset,
                     id="-".join((dataset, target)))
        for dtype in data_types
        for dataset in dataset_map[dtype]
        for target in target_map[dtype]
        if large or dataset not in dataset_map['large']
        if target not in ensure_list(exclude_targets)
    )
    return target_dir_pairs


def parametrize_target(large=True, exclude_targets=None):
    return pytest.mark.parametrize(
        "project_dir,target,project",
        get_targets(large, ensure_list(exclude_targets)),
        indirect=['project_dir', 'target'])


    # blast.rules
    #'blast_gene_find':   '{}.by_Subject.mhc.blast/query.q1.csv',
    # coverage rules
    #'coverage_bbm': '{}.by_Subject.mhc.bbm.cov/blast.query.q1.csv',
    #'coverage_bt2': '{}.by_Subject.mhc.bt2.cov/blast.query.q1.csv'
    # otu.rules
    # 'otu_table':         '{}.by_Subject.mhc.blast.otu/psa.wcfR.otu_table.csv'

