"""This module defines "Project", a Stage type defined by a project
matrix file giving units and meta data for input files.
"""

import logging
import os
import re
from collections.abc import Mapping, Sequence
import sqlite3

from typing import List, Union, Dict, Set, Optional

import ymp
from ymp.exceptions import YmpConfigError
from ymp.stage.base import ConfigStage
from ymp.util import is_fq, make_local_path

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class PandasTableBuilder:
    """Builds the data table describing each sample in a project

    This class implements loading and combining tabular data files
    as specified by the YAML configuration.

    Format:
     - string items are files
     - lists of files are concatenated top to bottom
     - dicts must have one "command" value:

       - 'join' contains a two-item list
         the two items are joined 'naturally' on shared headers
       - 'table' contains a list of one-item dicts
         dicts have form ``key:value[,value...]``
         a in-place table is created from the keys
         list-of-dict is necessary as dicts are unordered
       - 'paste' contains a list of tables pasted left to right
         tables pasted must be of equal length or length 1
     - if a value is a valid path relative to the csv/tsv/xls file's
       location, it is expanded to a path relative to CWD

    Example:
     .. code-block:: yaml

      - top.csv
      - join:
        - excel.xslx%left.csv
        - right.tsv
      - table:
        - sample: s1,s2,s3
        - fq1: s1.1.fq, s2.1.fq, s3.1.fq
        - fq2: s1.2.fq, s2.2.fq, s3.2.fq

    """
    def __init__(self):
        # Importing Pandas here to avoid long load time if we don't need it.
        # pylint: disable=import-outside-toplevel
        import pandas
        from pandas.core.reshape.merge import MergeError
        self.pd = pandas
        self.MergeError = MergeError
        self.files: List[str] = []

    def load_data(self, cfg, key):
        if not (key in cfg or isinstance(cfg, Sequence)):
            raise YmpConfigError(cfg, f"Missing key '{key}' in project data config", key=key)
        value = cfg[key]
        if isinstance(value, Sequence) and not isinstance(value, str):
            return self._rowbind(cfg, key)
        if isinstance(value, Mapping):
            command = next(iter(value), None)
            if len(value) != 1 or command not in ("join", "paste", "table"):
                raise YmpConfigError(cfg, "Expecting exactly one of join, paste or table", key=key)
            if command == "join":
                return self._join(value["join"])
            if command == "paste":
                return self._paste(value["paste"])
            if command == "table":
                return self._table(value["table"])
        return self._load_file(cfg, key)

    def _load_file(self, cfg, key):
        fname = cfg.get_path(key)
        try:
            data = self.pd.read_csv(
                fname, sep=None, engine='python', dtype='str'
            )
        except FileNotFoundError:
            parts = fname.split('%', maxsplit=1)
            try:
                data = self.pd.read_excel(
                    parts[0], parts[1] if len(parts) > 1 else 0)
            except ImportError as exc:
                raise YmpConfigError(
                    cfg,
                    "Could not load specified data file."
                    " If this is an Excel file, you might need"
                    " to install 'openpyxl'.",
                    key=key
                ) from exc
        # prefix fq files with name of config file's directory
        rdir = os.path.dirname(fname)
        data = data.map(
            lambda s: os.path.join(rdir, s)
            if is_fq(s) and os.path.exists(os.path.join(rdir, s))
            else s
        )
        self.files.append(cfg)
        return data

    def _rowbind(self, cfg, key):
        sequence = cfg[key]
        tables = list(map(self.load_data, [sequence]*len(sequence), range(len(sequence))))
        return self.pd.concat(tables, ignore_index=True)

    def _join(self, cfg):
        tables = list(map(self.load_data, [cfg]*len(cfg), range(len(cfg))))
        try:
            return self.pd.merge(*tables)
        except self.MergeError as e:
            raise YmpConfigError(
                cfg,
                "Failed to `join` configured data.\n"
                "Joined table indices:\n{}\n\n"
                "".format("\n".join(", ".join(table.columns.tolist())
                                    for table in tables))
            )

    def _paste(self, cfg):
        tables = list(map(self.load_data, [cfg]*len(cfg), range(len(cfg))))
        manyrow = [table for table in tables if len(table) > 1]
        if manyrow:
            nrows = len(manyrow[0])
            if any(len(table) != nrows for table in manyrow[1:]):
                raise YmpConfigError(
                    cfg,
                    "Failed to `paste` configured data. "
                    "Row counts differ and are not 1."
                    "Row counts: {}\n"
                    "".format(", ".join(str(len(table))
                                        for table in manyrow)))
            tables = [
                table if len(table) > 1
                else self.pd.concat([table]*nrows, ignore_index=True)
                for table in tables
            ]
        return self.pd.concat(tables, axis=1)

    def _table(self, cfg):
        return self.pd.DataFrame.from_dict({
            key: value.split(',')
            for row in cfg
            for key, value in row.items()
        })


class SQLiteProjectData(object):
    def __init__(self, cfg, key, name="data"):
        self.name = name
        self.conn = self._connect()
        table_builder = PandasTableBuilder()
        table_builder.load_data(cfg, key).to_sql(name, self.conn, index=False)

    @property
    def db_url(self):
        return f"file:{id(self)}?mode=memory"

    def _connect(self):
        return sqlite3.connect(self.db_url, uri=True, check_same_thread=False)

    def __getstate__(self):
        return (self.name, self.dump())

    def __setstate__(self, state):
        self.name = state[0]
        self.conn = self._connect()
        self.conn.executescript(state[1])

    def query(self, *args):
        try:
            return self.conn.execute(*args)
        except:  # noqa: E722
            log.error(f"Failed to query project {self.name} data: {args}")
            raise

    @property
    def nrows(self):
        return self.query(
            'SELECT COUNT(*) from "{}"'.format(self.name)
        ).fetchone()[0]

    def columns(self):
        return [t[1] for t in self.query(
            'PRAGMA table_info("{}")'.format(self.name)
        )]

    def identifying_columns(self):
        nrows = self.nrows
        return [t for t in self.columns() if self.query(
            'SELECT COUNT(DISTINCT "{}") FROM "{}"'.format(t, self.name)
        ).fetchone()[0] == nrows]

    def dump(self):
        return "\n".join(self.conn.iterdump())

    def duplicate_rows(self, column):
        return [t[0] for t in self.query("""
        SELECT "{c}" FROM "{n}" WHERE "{c}" in (
                 SELECT "{c}" FROM "{n}" GROUP BY "{c}" having COUNT("{c}") > 1
        )
        """.format(c=column, n=self.name)
        )]

    def string_columns(self):
        return [t[1] for t in self.query(
            'PRAGMA table_info("{}")'.format(self.name)
        ) if t[2] == 'TEXT']

    def rows(self, col):
        return self.query(
            'SELECT {c} from "{n}"'.format(
                n=self.name,
                c="0," + ",".join('"{}"'.format(c) for c in col)
            )
        )

    def fetch(
            self,
            cols: Union[List[str], str],
            idcols: Optional[Union[List[str], str]] = None,
            values: Optional[Union[List[str], str]] = None
    ) -> List[List[str]]:
        if isinstance(cols, str):
            cols = [cols]
        query = "SELECT DISTINCT " + ",".join(f'"{col}"' for col in cols)
        query += f" FROM {self.name}"
        if idcols is not None:
            if values is None:
                raise RuntimeError(f"idcols set ({idcols}) but values is not")
            if isinstance(idcols, str):
                idcols = [idcols]
            if isinstance(values, str):
                values = [values]
            if len(idcols) != len(values):
                log.error("len(%s) != len(%s)", idcols, values)
                raise RuntimeError("idcols and values must have same length")
            query += " WHERE "
            query += " AND ".join(f'"{col}"=?' for col in idcols)
        else:
            values = []
        return self.query(query, values).fetchall()

    def groupby_dedup(self, cols):
        skip = set()
        result = []
        for g in cols:
            if g in skip:
                continue
            result.append(g)
            q = 'SELECT {c} FROM "{n}" GROUP BY {g}'.format(
                n=self.name,
                g=",".join('"{}"'.format(g) for g in result),
                c=",".join(('COUNT(DISTINCT "{}")'.format(c) for c in cols))
            )
            counts = self.query(q).fetchall()
            unique = [all(x == 1 for x in n) for n in zip(*counts)]
            rcols = set(c for c, u in zip(cols, unique) if u)
            skip |= rcols
        return result


class Project(ConfigStage):
    """Contains configuration for a source dataset to be processed"""
    KEY_DATA = 'data'
    KEY_IDCOL = 'id_col'
    KEY_READCOLS = 'read_cols'
    KEY_BCCOL = 'barcode_col'

    # SRR columns:
    # LibraryLayout_s PAIRED
    # LibrarySelection_s PCR | RANDOM

    RE_REMOTE = re.compile(r"^(?:https?|ftp|sftp)://(?:.*)")
    RE_SRR = re.compile(r"^[SED]RR[0-9]+$")
    RE_FILE = re.compile(r"^(?!http://).*\.(?:fq|fastq)(?:|\.gz)$")

    def __init__(self, name, cfg):
        super().__init__(name, cfg)
        self.pairnames = ymp.get_config().pairnames
        self.fieldnames = None
        self._data = None
        self._source_cfg = None
        self._idcol = None
        self.bccol = cfg.get(self.KEY_BCCOL)

        if self.KEY_DATA not in self.cfg:
            raise YmpConfigError(
                self.cfg, "Missing key '{}'".format(self.KEY_DATA))

    def __repr__(self):
        return "{}(project={})".format(self.__class__.__name__, self.name)

    @property
    def outputs(self) -> Union[Set[str], Dict[str, str]]:
        return set(("/{sample}.R1.fq.gz", "/{sample}.R2.fq.gz",
                    "/{:samples:}.R1.fq.gz", "/{:samples:}.R2.fq.gz"))

    @property
    def data(self):
        """Pandas dataframe of runs

        Lazy loading property, first call may take a while.
        """
        if self._data is None:
            self._data = SQLiteProjectData(self.cfg, self.KEY_DATA)

        return self._data

    @property
    def variables(self):
        return self.data.columns()

    def minimize_variables(self, groups):
        """Removes redundant groupings
        """
        include = set(self.variables)
        known_groups = list(filter(lambda x: x in include, groups))
        other_groups = list(filter(lambda x: x not in include, groups))
        if len(known_groups) < 2:
            minimal_groups = known_groups
        else:
            # FIXME: dedup should work for arbitrary order!
            minimal_groups = self.data.groupby_dedup(known_groups)
            minimal_groups.reverse()
            minimal_groups = self.data.groupby_dedup(minimal_groups)
            minimal_groups.reverse()
        return minimal_groups, other_groups

    def get_group(
            self,
            stack: "StageStack",  # noqa: F821
            default_groups: List[str],
    ) -> List[str]:
        return super().get_group(stack, [self.idcol])

    def get_ids(self, stack, groups, match_groups=None, match_values=None):
        include = set(self.variables)
        if match_groups is not None:
            avail_group = []
            target_parts = []
            for group, target in zip(match_groups, match_values.split("__")):
                if group in include:
                    avail_group.append(group)
                    target_parts.append(target)
            match_groups = avail_group
            match_values = "__".join(target_parts)
        return super().get_ids(stack, groups, match_groups, match_values)

    def do_get_ids(self, _stack, groups, match_groups=None, match_values=None):
        if match_values:
            match_values = match_values.split("__")

        return ["__".join(t) for t in self.data.fetch(
            groups,
            match_groups,
            match_values
        )]

    def iter_samples(self, variables=None):
        if not variables:
            variables = self.variables
        return self.data.rows(variables)

    @property
    def runs(self):
        """Pandas dataframe index of runs

        Lazy loading property, first call may take a while.
        """
        return [run[0] for run in self.data.fetch(self.idcol)]

    @property
    def idcol(self):
        if self._idcol is None:
            self._idcol = self.choose_id_column()
        return self._idcol

    @property
    def source_cfg(self):
        if self._source_cfg is None:
            self._source_cfg = self.choose_fq_columns()
        return self._source_cfg

    def choose_id_column(self):
        """Configures column to use as index on runs

        If explicitly configured via KEY_IDCOL, verifies that the column
        exists and that it is unique. Otherwise chooses the leftmost
        unique column in the data.
        """
        all_columns = self.data.columns()
        unique_columns = self.data.identifying_columns()

        if not unique_columns:
            raise YmpConfigError(
                self.cfg,
                "Project data has no column containing unique values for "
                "each row. At least one is needed to identify samples!"
            )

        if self.KEY_IDCOL in self.cfg:
            idcol = self.cfg[self.KEY_IDCOL]
            if idcol not in all_columns:
                raise YmpConfigError(self.cfg, key=self.KEY_IDCOL, msg=(
                    "Configured column not found in data. "
                    "Possible spelling error? Available columns: "
                    ", ".join(all_columns)
                    ))

            if idcol not in unique_columns:
                raise YmpConfigError(self.cfg, key=self.KEY_IDCOL, msg=(
                    "Configured id_col column '{}' is not unique.\n"
                    "Duplicated rows:\n {}\n"
                    "Unique columns: {}".format(
                        idcol, self.data.duplicate_rows(idcol), unique_columns
                    )
                ))
        else:
            idcol = unique_columns[0]
            log.warning("Project '%s' using column '%s' to identify units",
                        self.name, idcol)

        return idcol

    def choose_fq_columns(self):
        """
        Configures the columns referencing the fastq sources
        """
        # get only columns containing string data
        string_cols = self.data.string_columns()

        # if barcode column specified, omit that
        if self.bccol:
            string_cols.remove(self.bccol)

        # if read columns specified, constrain to those
        read_cols = self.cfg.get(self.KEY_READCOLS)
        if read_cols:
            if isinstance(read_cols, str):
                read_cols = [read_cols]
            typo_cols = set(read_cols) - set(string_cols)
            if typo_cols:
                log.warning("%s=%s references invalid columns: %s",
                            self.KEY_READCOLS, read_cols, typo_cols)
                read_cols = [col for col in read_cols if col not in typo_cols]
        else:
            read_cols = string_cols

        if not read_cols:
            raise YmpConfigError(self.cfg, key=self.KEY_READCOLS, msg=(
                "No columns containing read files found"
            ))

        err = False
        source_config = {}
        for row in self.data.rows([self.idcol] + read_cols):
            cols = []
            for i, val in enumerate(row[2:]):
                if val is None:
                    val = ""
                if self.RE_FILE.match(val):
                    cols.append(("file", read_cols[i]))
                elif self.RE_REMOTE.match(val):
                    cols.append(("remote", read_cols[i]))
                elif self.RE_SRR.match(val):
                    cols.append(("srr", read_cols[i]))
            types = set(col[0] for col in cols)
            if not types:
                log.error("No data sources found in row %s.",
                          row[1])
                err = True
            elif len(types) > 1 or len(cols) > 2 or \
                 (cols[0] == 'srr' and len(cols) > 1):
                log.error("Ambiguous data sources found in row %s. "
                          "You may need to constrain the columns allowed "
                          "to contain read data using '%s'.",
                          row[1], self.KEY_READCOLS)
                err = True
            elif len(cols) == 2:
                source_config[row[1]] = (cols[0][0], cols[0][1], cols[1][1])
            elif len(cols) == 1:
                source_config[row[1]] = (cols[0][0], cols[0][1], None)
            else:
                raise RuntimeError("this should not have happened")
        if err:
            raise YmpConfigError(self.cfg, msg=(
                "Failed to identify source data in project data config. "
                "See above log messages for details."
            ))

        return source_config

    def raw_reads_source_path(self, args, kwargs):
        return self.source_path(*args)

    def source_path(self, target, pair, nosplit=False):
        """Get path for FQ file for ``run`` and ``pair``"""
        source = self.source_cfg.get(target)
        cfg = ymp.get_config()
        if not source:
            raise YmpConfigError(self.cfg,
                                 "No run '{}' in source config".format(target))

        if isinstance(pair, str):
            pair = self.pairnames.index(pair)

        if self.bccol and not nosplit:
            barcode_file, = self.data.fetch(self.bccol, self.idcol, target)[0]
            if barcode_file:
                return self.encode_barcode_path(barcode_file, target, pair)

        kind = source[0]
        if kind == 'srr':
            srr, = self.data.fetch(source[1], self.idcol, target)[0]
            f = os.path.join(cfg.dir.scratch,
                             "SRR",
                             "{}_{}.fastq.gz".format(srr, pair+1))
            return f

        fq_col = source[pair+1]
        if not isinstance(fq_col, str):
            return (
                "Configuration Error: no source for sample {} and read {} "
                "found.".format(target, pair+1))

        fn, = self.data.fetch(fq_col, self.idcol, target)[0]
        if kind == 'file':
            return fn

        if kind == 'remote':
            return make_local_path(cfg, fn)

        raise YmpConfigError(
            self.cfg,
            "Configuration Error: no source for sample {} and read {} found."
            "".format(target, pair+1))

    def encode_barcode_path(self, barcode_file, run, pair):
        if barcode_file:
            barcode_id = barcode_file.replace("_", "__").replace("/", "_x_")
            return (
                "{project}.split_libraries/{barcodes}/{run}.{pair}.fq.gz"
                "".format(
                    project=self.name,
                    barcodes=barcode_id,
                    run=run,
                    pair=self.pairnames[pair])
            )

    def unsplit_path(self, barcode_id, pairname):
        barcode_file = barcode_id.replace("_x_", "/").replace("__", "_")
        pair = self.pairnames.index(pairname)

        run, = self.data.fetch(self.idcol, self.bccol, barcode_file)[0]
        source = self.source_path(run, pair, nosplit=True)
        return [barcode_file, source]

    def get_all_targets(self, stack: "StageStack", output_types=None) -> List[str]:
        path = stack.path
        return [os.path.join(path, "{}.fq.gz".format(base))
                for base in self.get_fq_names()]

    def get_fq_names(self,
                     only_fwd=False, only_rev=False,
                     only_pe=False, only_se=False):
        """Get pipeline names of fq files"""

        if only_fwd and only_rev:  # pointless, but satisfiable
            return []
        if only_pe and only_se:  # equally pointless, zero result
            return []

        pairs = []
        if not only_rev:
            pairs += [0]
        if not only_fwd:
            pairs += [1]

        check_rev = only_pe or only_se

        def have_file(run, pair):
            return (isinstance(self.source_cfg[run][pair+1], str)
                    or self.source_cfg[run][0] == 'srr')

        return [
            "{}.{}".format(run, self.pairnames[pair])
            for run in self.runs
            for pair in pairs
            if have_file(run, pair)
            if not check_rev or have_file(run, 1) == only_pe
        ]

    @property
    def fq_names(self):
        "Names of all FastQ files"
        try:
            return self.get_fq_names()
        except:
            log.exception("hetre")
            raise

    @property
    def pe_fq_names(self):
        "Names of paired end FastQ files"
        return self.get_fq_names(only_pe=True)

    @property
    def se_fq_names(self):
        "Names of single end FastQ files"
        return self.get_fq_names(only_se=True)

    @property
    def fwd_pe_fq_names(self):
        "Names of forward FastQ files part of pair"
        return self.get_fq_names(only_pe=True, only_fwd=True)

    @property
    def rev_pe_fq_names(self):
        "Names of reverse FastQ files part of pair"
        return self.get_fq_names(only_pe=True, only_rev=True)

    @property
    def fwd_fq_names(self):
        "Names of forward FastQ files (se and pe)"
        return self.get_fq_names(only_fwd=True)

    @property
    def project_name(self):
        return self.name
