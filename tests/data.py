from ymp.common import odict

# test subject : build target
targets = odict[
    # fastq.rules
    'import':         'reports/{}_qc.html',
    'BBMap_ecco':     'reports/{}.ecco_qc.html',
    'BBDuk_trim':     'reports/{}.trimAQ10_qc.html',
    'BBDuk_remove':   'reports/{}.bbmRMphiX_qc.html',
    'BB_dedupe':      'reports/{}.ddp_qc.html',
    'phyloFlash':     'reports/{}_heatmap.pdf',
    'trimmomaticT32': 'reports/{}.trimmomaticT32_qc.html',
    'sickle':         'reports/{}.sickle_qc.html',
    'sickleQ10':      'reports/{}.sickleQ10_qc.html',
    'sickleL10':      'reports/{}.sickleL10_qc.html',
    'sickleQ10L10':   'reports/{}.sickleQ10L10_qc.html',
    'bmtagger':       'reports/{}.bmtaggerRMphiX_qc.html',
    # assembly.rules
    'assemble_separate_mh': 'reports/{}.by_ID.mhc.mq.html',
    'assemble_grouped_mh':  'reports/{}.by_Subject.mhc.mq.html',
    'assemble_joined_mh':   'reports/{}.mhc.mq.html',
    'assemble_separate_sp': 'reports/{}.by_ID.sp.mq.html',
    'assemble_grouped_sp':  'reports/{}.by_Subject.sp.mq.html',
    'assemble_joined_sp':   'reports/{}.sp.mq.html',
    # mapping.rules
    'map_bbmap_separate':   '{}.by_ID.mhc.bbm/complete',
    'map_bbmap_grouped':   '{}.by_Subject.mhc.bbm/complete',
    'map_bbmap_joined':   '{}.mhc.bbm/complete',
    'map_bowtie2_separate': '{}.by_ID.mhc.bt2/complete',
    'map_bowtie2_grouped': '{}.by_Subject.mhc.bt2/complete',
    'map_bowtie2_joined': '{}.mhc.bt2/complete',
    # blast.rules
    'blast_gene_find':   '{}.by_Subject.mhc.blast/psa.wcfR.csv',
    # coverage rules
    'coverage':          '{}.by_Subject.mhc.bbm.cov/blast.psa.wcfR.csv',
    'coverage':          '{}.by_Subject.mhc.bt2.cov/blast.psa.wcfR.csv'
    # otu.rules
    # 'otu_table':         '{}.by_Subject.mhc.blast.otu/psa.wcfR.otu_table.csv'
]
