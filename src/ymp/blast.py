"""
Parsers for blast output formats 6 (CSV) and 7 (CSV with comments between queries).
"""

import logging

from collections import namedtuple
from typing import List


log = logging.getLogger(__name__)  # pylint: disable=invalid-name


def reader(fileobj, t: int=7) -> 'BlastParser':
    """
    Creates a reader for files in BLAST format

    >>> with open(blast_file) as infile:
    >>>    reader = blast.reader(infile)
    >>>    for hit in reader:
    >>>       print(hit)

    Args:
      fileobj: iterable yielding lines in blast format
      t: number of blast format type
    """
    if t == 7:
        return Fmt7Parser(fileobj)
    elif t == 6:
        return Fmt6Parser(fileobj)
    else:
        raise NotImplementedError()


def writer(fileobj, t: int=7) -> 'BlastWriter':
    """
    Creates a writer for files in BLAST format

    >>> with open(blast_file) as outfile:
    >>>    writer = blast.writer(outfile)
    >>>    for hit in hits:
    >>>       writer.write_hit(hit)
    """
    if t == 7:
        return Fmt7Writer(fileobj)
    else:
        raise NotImplementedError()


class BlastBase(object):
    "Base class for BLAST readers and writers"

    def tupleofint(text):
        if text == "N/A":
            return tuple()
        try:
            return tuple(int(i) for i in text.split(';'))
        except ValueError:
            log.warning(f"Error parsing BLAST file at line='{text}'")
            return tuple()

    #: Map between field short and long names
    FIELD_MAP = {
        #"": "qseqid",  # Query Seq-id
        #"": "qgi",  # Query GI
        "query acc.": "qacc",  # Query accession
        #"": "qaccver",  # Query accession.version
        "query length": "qlen",  # Query sequence length
        #"": "sseqid",  # Subject Seq-id
        #"": "sallseqid",  # All subject Seq-id(s), separated by ';'
        #"": "sgi",  # Subject GI
        #"": "sallgi",  # All subject GIs
       "subject acc.": "sacc",  # Subject accession
        #"": "saccver",  # Subject accession.version
        #"": "sallacc",  # All subject accessions
        #"": "slen",  # Subject sequence length
        "q. start": "qstart",  # Start of alignment in query
        "q. end": "qend",  # End of alignment in query
        "s. start": "sstart",  # Start of alignment in subject
        "s. end": "send",  # Start of alignment in query
        #"": "qseq",  # Aligned part of query sequence
        #"": "sseq",  # Aligned part of subject sequence
        "evalue": "evalue",  # Expect value
        "bit score": "bitscore",  # Bit score
        "score": "score",  # Raw score
        "alignment length": "length",  # Alignment length
        "% identity": "pident",  # Percentage of identical matches
        "mismatches": "mismatch",  # Number of mismatches
        #"": "positive", # Number of positive-scoring matches
        "gap opens": "gapopen", # Number of gap openings
        #"": "gaps",  # Total number of gaps
        #"": "ppos",  # Percentage of positive-soring matches
        #"": "frames",  # Query and subject frames separated by a '/'
        "query frame": "qframe",  # Query frame
        "sbjct frame": "sframe",  # Subject frame
        #"": "btop",  # Blast traceback operations (BTOP)
        #"": "staxid",  # Subject Taxonomy ID
        #"": "scciname",  # Subject Scientifi Name
        #"": "scomname",  # Subject Common Name
        #"": "sblastname",  # Subject Blast Name
        #"": "sskingdom",  # Subject Super Kingdom
        "subject tax ids": "staxids",  # sorted unique ';'-separated Subject Taxonomy ID(s)
        #"": "sscinames",  # unique Subject Scientific Name(s)
        #"": "scomnames",  # unique Subject Common Name(s)
        #"": "sblastnames",  # unique Subject Blast Name(s)
        #"": "sskingdoms",  # unique Subject Super Kingdom(s)
        "subject title": "stitle",  # Subject Title
        #"": "sakktutkes",  # All Subject Title(s) separated by '<>'
        "subject strand": "sstrand",  # Subject Strand
        #"": "qcovs",  # Query Coverage per Subject
        #"": "qcovhsp",  # Query Coverage per HSP
        #"": "qcovus",  # Query Coverage per Unique Subject (blastn only)
    }

    #: Reversed map from short to long name
    FIELD_REV_MAP = {
        value: key
        for key, value in FIELD_MAP.items()
    }

    #: Map defining types of fields
    FIELD_TYPE = {
        'pident': float,
        'length': int,
        'mismatch': int,
        'gapopen': int,
        'qstart': int,
        'qend': int,
        'qlen': int,
        'sstart': int,
        'send': int,
        'evalue': float,
        'bitscore': float,
        'score': float,
        'sframe': int,
        'qframe': int,
        'stitle': str,
        'staxids': tupleofint
    }


class BlastParser(BlastBase):
    """Base class for BLAST readers"""
    def get_fields(self):
        raise NotImplementedError()

    def __iter__(self):
        raise NotImplementedError()


class BlastWriter(BlastBase):
    """Base class for BLAST writers"""
    def write_hit(self, hit):
        raise NotImplementedError()


class Fmt7Parser(BlastParser):
    """
    Parses BLAST results in format '7' (CSV with comments)
    """
    PAT_FIELDS = "# Fields: "
    PAT_QUERY = "# Query: "
    PAT_DATABASE = "# Database: "
    PAT_HITSFOUND = " hits found"

    def __init__(self, fileobj):
        self.fileobj = fileobj
        self.fields = None
        self.query = "undefined"
        self.database = "undefined"
        if "BLAST" not in fileobj.readline():
            raise ValueError("not a BLAST7 formatted file")

    def get_fields(self) -> List[str]:
        """Returns list of available field names

        Format 7 specifies which columns it contains in comment lines, allowing
        this parser to be agnostic of the selection of columns made when running
        BLAST.

        Returns:
          List of field names (e.g. ``['sacc', 'qacc', 'evalue']``)
        """
        return self.fields

    def __iter__(self):
        for line in self.fileobj:
            if line.startswith(self.PAT_FIELDS):
                self.fields = [
                    self.FIELD_MAP[field]
                    if field in self.FIELD_MAP else field
                    for field in line[len(self.PAT_FIELDS):].strip().split(", ")
                ]
                self.Hit = namedtuple("BlastHit", self.fields)
            elif line.startswith(self.PAT_QUERY):
                self.query = line[len(self.PAT_QUERY):].strip()
            elif line.startswith(self.PAT_DATABASE):
                self.database = line[len(self.PAT_DATABASE):].strip()
            elif line.strip().endswith(self.PAT_HITSFOUND):
                self.hits = int(line.split()[1])
                self.hit = 0
            elif line[0] == "#":
                continue
            else:
                self.hit += 1
                yield self.Hit(*[
                    self.FIELD_TYPE[key](value)
                    if key in self.FIELD_TYPE else value
                    for key, value in zip(self.fields,
                                          line.strip().split('\t'))
                ])

    def isfirsthit(self) -> bool:
        """Returns `True` if the current hit is the first hit for the current
        query"""
        return self.hit == 1


class Fmt6Parser(BlastParser):
    """Parser for BLAST format 6 (CSV)
    """
    #: Default field types
    fields = ("qseqid sseqid pident length mismatch gapopen "
              "qstart qend sstart send evalue bitscore").split()
    field_types = [BlastParser.FIELD_TYPE.get(n, None) for n in fields ]
    Hit = namedtuple("BlastHit", fields)
    def __init__(self, fileobj):
        self.fileobj = fileobj

    def get_fields(self):
        return self.fields

    def __iter__(self):
        for line in self.fileobj:
            yield self.Hit(*[t(v) if t else v
                             for v, t in zip(line.split("\t"),
                                             self.field_types)])


class Fmt7Writer(BlastWriter):
    def __init__(self, fileobj):
        self.fileobj = fileobj
        self.toolname = "YMP writer " + ymp.version
        self.query = None
        self.database = "undefined"
        self.fields = "undefined"
        self.hits = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, ext_value, tb):
        self.write_hitset()

    def write_header(self):
        """Writes BLAST7 format header"""
        self.fileobj.write(
            f"# {self.toolname}\n"
            F"# Query: {self.query}\n"
            f"# Database: {self.database}\n"
            f"# Fields: {self.fields}\n"
        )

    def write_hitset(self):
        self.query = self.hits[0].qacc
        self.fields = self.hits[0]._fields
        self.write_header()
        self.fileobj.write(f"# {len(self.hits)} found")

    def write_hit(self, hit):
        if self.hits and hit.qacc != self.hits[0].qacc:
            self.write_hitset()
        self.hits.append(hit)
