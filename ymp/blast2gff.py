#!/usr/bin/env python3

import blast
import gff
import click


@click.command()
@click.argument('input', type=click.File('r'))
@click.argument('output', type=click.File('w'))
def blast2gff(input, output):
    blastfile = blast.reader(input)
    gfffile = gff.writer(output)

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
    blast2gff()  # pylint: no-value-for-parameter
