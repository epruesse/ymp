"""
Parsers for blast output formats 6 (CSV) and 7 (CSV with comments between queries).
"""

from collections import namedtuple
from typing import List


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
        ValueError("other formats not implemented")


class BlastParser(object):
    "Base class for BLAST parsers"

    # Map between field short and long names
    FIELD_MAP = {
        "query acc.": "qacc",
        "subject acc.": "sacc",
        "% identity": "pident",
        "alignment length": "length",
        "mismatches": "mismatch",
        "gap opens": "gapopen",
        "q. start": "qstart",
        "q. end": "qend",
        "s. start": "sstart",
        "s. end": "send",
        "evalue": "evalue",
        "bit score": "bitscore",
        "subject strand": "sstrand",
        "sbjct frame": "sframe",
        "score": "score"
    }

    # Map defining types of fields
    FIELD_TYPE = {
        'pident': float,
        'length': int,
        'mismatch': int,
        'gapopen': int,
        'qstart': int,
        'qend': int,
        'sstart': int,
        'send': int,
        'evalue': float,
        'bitscore': float,
        'score': float,
        'sframe': int
    }


class Fmt7Parser(BlastParser):
    """
    Parses BLAST results in format '7' (CSV with comments)
    """
    FIELDS = "# Fields: "
    QUERY = "# Query: "
    DATABASE = "# Database: "
    HITSFOUND = " hits found"

    def __init__(self, fileobj):
        self.fileobj = fileobj
        self.fields = None
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
            if line.startswith(self.FIELDS):
                self.fields = [
                    self.FIELD_MAP[field]
                    if field in self.FIELD_MAP else field
                    for field in line[len(self.FIELDS):].strip().split(", ")
                ]
                self.Hit = namedtuple("BlastHit", self.fields)
            elif line.startswith(self.QUERY):
                self.query = line[len(self.QUERY):].strip()
            elif line.startswith(self.DATABASE):
                self.query = line[len(self.DATABASE):].strip()
            elif line.strip().endswith(self.HITSFOUND):
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

