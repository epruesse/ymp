from ymp.common import odict
from collections import OrderedDict

# test subject : build target
targets_all = odict[
    # fastq.rules
    'import':         '{}/all',
    'BBMap_ecco':     '{}.ecco/all',
    'BBDuk_trim':     '{}.trimAQ10/all',
    'BBDuk_remove':   '{}.bbmRMphiX/all',
    'BB_dedupe':      '{}.ddp/all',
    'phyloFlash':     'reports/{}_heatmap.pdf',
    'fastqc':         '{}.fastqc/all',
    'multiqc':        'reports/{}.fastqc.html',
    'trimmomaticT32': '{}.trimmomaticT32/all',
    'sickle':         '{}.sickle/all',
    'sickleQ10':      '{}.sickleQ10/all',
    'sickleL10':      '{}.sickleL10/all',
    'sickleQ10L10':   '{}.sickleQ10L10/all',
]

targets_metagenome = odict[
    # can't run bmtagger with less than 9GB RAM
    # 'bmtagger':       '{}.bmtaggerRMphiX/all',
    # assembly.rules
    'assemble_separate_mh': '{}.by_ID.mhc/all',
    'assemble_grouped_mh':  '{}.by_Subject.mhc/all',
    'assemble_joined_mh':   '{}.mhc/all',
    'assemble_separate_sp': '{}.by_ID.sp/all',
    'assemble_grouped_sp':  '{}.by_Subject.sp/all',
    'assemble_joined_sp':   '{}.sp/all',
    'metaquast_mh':         'reports/{}.mhc.mq.html',
    'metaquast_sp':         'reports/{}.sp.mq.html',
    # mapping.rules
    'map_bbmap_separate':   '{}.by_ID.mhc.bbm/all',
    'map_bbmap_grouped':   '{}.by_Subject.mhc.bbm/all',
    'map_bbmap_joined':   '{}.mhc.bbm/all',
    'map_bowtie2_separate': '{}.by_ID.mhc.bt2/all',
    'map_bowtie2_grouped': '{}.by_Subject.mhc.bt2/all',
    'map_bowtie2_joined': '{}.mhc.bt2/all',
]

targets_amplicon = OrderedDict()

    # blast.rules
    #'blast_gene_find':   '{}.by_Subject.mhc.blast/query.q1.csv',
    # coverage rules
    #'coverage_bbm': '{}.by_Subject.mhc.bbm.cov/blast.query.q1.csv',
    #'coverage_bt2': '{}.by_Subject.mhc.bt2.cov/blast.query.q1.csv'
    # otu.rules
    # 'otu_table':         '{}.by_Subject.mhc.blast.otu/psa.wcfR.otu_table.csv'

targets = OrderedDict()
targets.update(targets_all)
targets.update(targets_metagenome)
targets.update(targets_amplicon)
