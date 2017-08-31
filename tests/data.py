from ymp.common import odict

# test subject : build target
targets = odict[
    # fastq.rules
    'import':         '{}/all',
    'BBMap_ecco':     '{}.ecco/all',
    'BBDuk_trim':     '{}.trimAQ10/all',
    'BBDuk_remove':   '{}.bbmRMphiX/all',
    'BB_dedupe':      '{}.ddp/all',
    'phyloFlash':     'reports/{}_heatmap.pdf',
    'multiqc':        'reports/{}_qc.html',
    'trimmomaticT32': '{}.trimmomaticT32/all',
    'sickle':         '{}.sickle/all',
    'sickleQ10':      '{}.sickleQ10/all',
    'sickleL10':      '{}.sickleL10/all',
    'sickleQ10L10':   '{}.sickleQ10L10/all',
    'bmtagger':       '{}.bmtaggerRMphiX/all',
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
    'blast_gene_find':   '{}.by_Subject.mhc.blast/query.q1.csv',
    # coverage rules
    'coverage_bbm': '{}.by_Subject.mhc.bbm.cov/blast.query.q1.csv',
    'coverage_bt2': '{}.by_Subject.mhc.bt2.cov/blast.query.q1.csv'
    # otu.rules
    # 'otu_table':         '{}.by_Subject.mhc.blast.otu/psa.wcfR.otu_table.csv'
]
