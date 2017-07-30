"""
Various converters

"""

def BlastHit2GffFeature(blasthit):
    return gff.Feature(
        seqid = hit.sacc,
        source = 'BLAST',
        type = 'CDS',
        start = min(hit.sstart, hit.send),
        end = max(hit.sstart, hit.send),
        score = hit.evalue,
        strand = '+' if hit.sframe > 0 else '-',
        phase = '0',
        attributes = "ID={}_{}_{}".format(hit.sacc, hit.sstart, hit.send)
    )
