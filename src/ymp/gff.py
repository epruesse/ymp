"""
Implements simple reader and writer for GFF (general feature format) files.

Unfinished

 - only supports one version, GFF 3.2.3.
 - no escaping
"""


from collections import namedtuple

_FIELDS = [
    'seqid',   # the sequence id
    'source',  # the tool
    'type',    # CDS, rRNA, ...
    'start',
    'end',
    'score',
    'strand',  # + / -
    'phase',
    'attributes'
]

Feature = namedtuple("Feature", _FIELDS)

_ATTRIBUTES = [
    'ID',      # unique ID per GFF, use multiple for discontinuous features
    'Name',    # display name for user
    'Alias',   # secondary name, e.g. accession
    'Parent',  # indicate "partof" relationship
    'Target',  # alignment target to 'target_id start end [strand]'
    'Gap',     # alignment in CIGAR format
    'Derives_From',  # temporal relationship
    'Note',    # free text
    'Dbxref',  # database cross reference
    'Ontology_term',
    'Is_circular',  # if feature is circular
    # note
    # product
    # partial
    # start_type
    # rbs_motif
    # rbs_spacer
    # gc_cont
    # conf
    # score
    # <x>score
]

Attributes = namedtuple("Attributes", _ATTRIBUTES)


class reader(object):
    def __init__(self, fileobj):
        self.fileobj = fileobj

    def __iter__(self):
        for line in self.fileobj:
            if isinstance(line, bytes):
                line = line.decode('ascii')
            if line[0] == "#":
                continue
            f = line.strip().split('\t')
            f[3] = int(f[3])
            f[4] = int(f[4])
            f[-1] = dict([
                tuple(item.split('='))
                for item in f[-1].strip(';').split(';')
            ])
            yield Feature(*f)


class writer(object):
    def __init__(self, fileobj):
        self.fileobj = fileobj
        self.fileobj.write("##gff-version 3\n")

    def write(self, feature):
        self.fileobj.write("\t".join(map(str, feature))+"\n")
