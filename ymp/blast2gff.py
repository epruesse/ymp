#!/usr/bin/env python3

import click
from ymp import blast
from ymp import gff


@click.command()
@click.argument('input', 'inf', type=click.File('r'))
@click.argument('output', 'out', type=click.File('w'))
def blast2gff(inf, out):
    blastfile = blast.reader(inf)
    gfffile = gff.writer(out)

    for hit in blastfile:
        print(type(hit))
        assert (hit.send > hit.sstart) == (hit.sframe > 0)

        feature = gff.Feature(
            seqid=hit.sacc,
            source='BLAST',
            type='CDS',
            start=min(hit.sstart, hit.send),
            end=max(hit.sstart, hit.send),
            score=hit.evalue,
            strand='+' if hit.sframe > 0 else '-',
            phase='0',
            attributes="ID={}_{}_{}".format(hit.sacc, hit.sstart, hit.send)
        )
        gfffile.write(feature)


if __name__ == '__main__':
    # pylint does not get click decorators, disable warning:
    # pylint: disable=no-value-for-parameter
    blast2gff()
