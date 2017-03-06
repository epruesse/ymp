AA = 'FFLLSSSSYY**CC*WLLLLPPPPHHQQRRRRIIIMTTTTNNKKSSRRVVVVAAAADDEEGGGG'
NU = 'TCAG'
B2N = { a:b for a,b in zip(NU, range(len(NU))) }
nuc2num = lambda seq: sum([
    len(NU) ** pos * B2N[nuc]
    for pos,nuc in enumerate(reversed(seq))
])
nuc2aa = lambda seq: ''.join([
    AA[nuc2num(codon)]
    for codon in zip(*[iter(seq)]*3)
])




