#!/usr/bin/env python3

import click


AA = 'FFLLSSSSYY**CC*WLLLLPPPPHHQQRRRRIIIMTTTTNNKKSSRRVVVVAAAADDEEGGGG'
NU = 'TCAG'
B2N = {a: b for a, b in zip(NU, range(len(NU)))}


def nuc2num(seq):
    return sum([
        len(NU) ** pos * B2N[nuc]
        for pos, nuc in enumerate(reversed(seq))
    ])


def nuc2aa(seq):
    return ''.join([
        AA[nuc2num(codon)]
        for codon in zip(*[iter(seq)]*3)
    ])


@click.command()
@click.argument('input', type=click.File('r'))
@click.argument('output', type=click.File('w'))
def click_fasta_dna2aa(input, output):
    if input.name.endswith(".gz"):
        import gzip
        input = gzip.open(input.name, "rt")
    fasta_dna2aa(input, output)


def fasta_dna2aa(inf, outf):
    def write_aa(header, seq):
        # outf.write(header.encode('ascii'))
        outf.write(header)
        aa = nuc2aa(seq)
        if "start_type=GTG" in header:
            aa = 'M'+aa[1:]
        outf.write(('\n'.join([
            aa[s:s+60]
            for s in range(0, len(aa)+59, 60)
            ]).strip()+'\n')
            # .encode('ascii')
        )

    header = None
    seq = ""

    for line in inf:
        # line = line.decode('ascii')
        if line[0] == '>':
            if header:
                write_aa(header, seq)
            header = line
            seq = ""
        else:
            seq += line.strip()
    if header:
        write_aa(header, seq)


if __name__ == "__main__":
    # pylint does not get click decorators, disable warning:
    # pylint: disable=no-value-for-parameter
    click_fasta_dna2aa()
