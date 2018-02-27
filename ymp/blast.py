from collections import namedtuple


def reader(fileobj, t=7):
    if t == 7:
        return fmt7_parser(fileobj)
    else:
        ValueError("other formats not implemented")


class blast_parser(object):
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


class fmt7_parser(blast_parser):
    """
    Parses BLAST results in fmt7 (CSV with comments)
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

    def get_fields(self):
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

    def isfirsthit(self):
        return self.hit == 1
