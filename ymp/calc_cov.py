#!/usr/bin/env python3

import logging
import pprint
from logging import info

import click

import pysam

from ymp import blast

pprinter = pprint.PrettyPrinter()
pprint = pprinter.pprint

logging.basicConfig(
    level=logging.INFO,
    format="%(relativeCreated)6.1f %(funcName)s: %(message)s",
    datefmt="%I:%M:%S"
)


@click.command()
@click.argument('bamfile', type=click.File('rb'))
@click.argument('regionfile', type=click.File('r'))
def calc_cov(bamfile, regionfile):
    info("Gathering coverages for regions in {}".format(regionfile.name))
    blastfile = blast.reader(regionfile, v=7)

    info("Processing bamfile: {}".format(bamfile.name))
    bam = pysam.AlignmentFile(bamfile)

    info("Num Reads (mapped/unmapped): {} ({}/{}, {}%)".format(
        bam.mapped + bam.unmapped, bam.mapped, bam.unmapped,
        bam.mapped / (bam.mapped + bam.unmapped) * 100
    ))

    name2ref = {word.split()[0]: word for word in bam.references}
    for hit in blastfile:
        # if blastfile.isfirsthit():

        ref = name2ref[hit.sacc]
        start, end = sorted((hit.sstart, hit.send))
        covarr = bam.count_coverage(ref, start, end, quality_threshold=0)
        cov = sum([sum(x) for x in covarr]) / abs(hit.sstart - hit.send)
        reads = bam.count(ref, start, end)
        rpmk = reads / ((end - start) / 1000.0)
        info("%s: %f / %f -- %f", hit.sacc, cov, rpmk, rpmk/cov)


# def outtake():
#    hist = []
#    for col in bam.pileup(ref, hit.sstart, hit.send):
#        if col.reference_pos < hit.sstart: continue
#        if col.reference_pos > hit.send: continue
#        hist.append(col.nsegments)
#    info(hist)

if __name__ == "__main__":
    # pylint does not get click decorators, disable warning:
    # pylint: disable=no-value-for-parameter
    calc_cov()
