import logging
import os
import re
from collections import Mapping, Sequence

from ymp.exceptions import YmpConfigError
from ymp.stage import Stage
from ymp.util import is_fq, make_local_path

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


def load_data(cfg):
    """Recursively loads csv/tsv type data as defined by yaml structure

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
        - bottom_left.csv
        - bottom_right.csv
      - table:
        - sample: s1,s2,s3
        - fq1: s1.1.fq, s2.1.fq, s3.1.fq
        - fq2: s1.2.fq, s2.2.fq, s3.2.fq

    """
    import pandas as pd
    from pandas.core.reshape.merge import MergeError

    if isinstance(cfg, str):
        try:
            data = pd.read_csv(cfg, sep=None, engine='python', dtype='str')
        except FileNotFoundError:
            parts = cfg.split('%')
            try:
                data = pd.read_excel(parts[0],
                                     parts[1] if len(parts) > 1 else 0)
            except ImportError:
                raise YmpConfigError(
                    cfg,
                    "Could not load specified data file."
                    " If this is an Excel file, you might need"
                    " to install 'xlrd'."
                )
        rdir = os.path.dirname(cfg)
        data = data.applymap(
            lambda s: os.path.join(rdir, s)
            if is_fq(s) and os.path.exists(os.path.join(rdir, s))
            else s)
        return data

    if isinstance(cfg, Sequence):
        return pd.concat(list(map(load_data, cfg)), ignore_index=True)
    if isinstance(cfg, Mapping):
        # JOIN
        if 'join' in cfg:
            tables = list(map(load_data, cfg['join']))
            try:
                return pd.merge(*tables)
            except MergeError as e:
                raise YmpConfigError(
                    cfg,
                    "Failed to `join` configured data.\n"
                    "Joined table indices:\n{}\n\n"
                    "".format("\n".join(", ".join(table.columns.tolist())
                                        for table in tables)),
                    exc=e)
        # PASTE
        if 'paste' in cfg:
            tables = list(map(load_data, cfg['paste']))
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
                    else pd.concat([table]*nrows, ignore_index=True)
                    for table in tables
                ]
            return pd.concat(tables, axis=1)
        # TABLE
        if 'table' in cfg:
            return pd.DataFrame.from_dict({
                key: value.split(',')
                for row in cfg['table']
                for key, value in row.items()
            })
    raise YmpConfigError(cfg, "Unrecognized statement in data config")


class Project(Stage):
    """Contains configuration for a source dataset to be processed"""
    KEY_DATA = 'data'
    KEY_IDCOL = 'id_col'
    KEY_READCOLS = 'read_cols'
    KEY_BCCOL = 'barcode_col'

    # SRR columns:
    # LibraryLayout_s PAIRED
    # LibrarySelection_s PCR | RANDOM

    RE_REMOTE = re.compile(r"^(?:https?|ftp|sftp)://(?:.*)")
    RE_SRR = re.compile(r"^SRR[0-9]+$")
    RE_FILE = re.compile(r"^(?!http://).*(?:fq|fastq)(?:|\.gz)$")

    def __init__(self, cfgmgr, project, cfg):
        # Fixme: put line in config here
        self.filename = "fn"
        self.lineno = 0
        # triggers early workflow load, breaking things... :/
        # super().__init__(project)
        self.project = project
        self.name = project
        self.altname = None
        self.cfgmgr = cfgmgr
        self.cfg = cfg
        self.fieldnames = None
        self._runs = None
        self._source_cfg = None
        self._idcol = None
        self.outputs = set(("/{sample}.R1.fq.gz", "/{sample}.R2.fq.gz",
                            "/{:samples:}.R1.fq.gz", "/{:samples:}.R2.fq.gz"))
        self.inputs = set()

        if self.KEY_DATA not in self.cfg:
            raise YmpConfigError(
                self.cfg, "Missing key '{}'".format(self.KEY_DATA))

    def __repr__(self):
        return "{}(project={})".format(self.__class__.__name__, self.project)

    @property
    def run_data(self):
        """Pandas dataframe of runs

        Lazy loading property, first call may take a while.
        """
        if self._runs is None:
            self._runs = load_data(self.cfg[self.KEY_DATA])
            self.choose_id_column()

        return self._runs

    @property
    def runs(self):
        """Pandas dataframe index of runs

        Lazy loading property, first call may take a while.
        """
        return self.run_data.index

    def choose_id_column(self):
        """Configures column to use as index on runs

        If explicitly configured via KEY_IDCOL, verifies that the column
        exists and that it is unique. Otherwise chooses the leftmost
        unique column in the data.
        """
        import pandas as pd

        column_frequencies = self._runs.apply(pd.Series.nunique)
        log.debug("Column frequencies: {}".format(column_frequencies))
        nrows = self._runs.shape[0]
        log.debug("Row count: {}".format(nrows))
        unique_columns = self._runs.columns[column_frequencies == nrows]

        if unique_columns.empty:
            raise YmpConfigError(
                self.cfg,
                "Project data has no column containing unique values for "
                "each row. At least one is needed to identify samples!"
            )

        if self.KEY_IDCOL in self.cfg:
            idcol = self.cfg[self.KEY_IDCOL]
            if idcol not in self._runs.columns:
                raise YmpConfigError(
                    self.cfg, key=self.KEY_IDCOL,
                    msg="Configured column not found in data. "
                    "Possible spelling error? "
                    "Available columns: " +
                    ", ".join(str(c) for c in self._runs.columns))

            if idcol not in unique_columns:
                duplicated = self._runs.duplicated(subset=[idcol], keep=False)
                dup_rows = self._runs[duplicated].sort_values(by=idcol)
                raise YmpConfigError(
                    self.cfg, key=self.KEY_IDCOL,
                    msg="Configured id_col column '{}' is not unique.\n"
                    "Duplicated rows:\n {}\n"
                    "Unique columns: {}"
                    "".format(
                        idcol, dup_rows, list(unique_columns)
                    )
                )
        else:
            self.cfg[self.KEY_IDCOL] = unique_columns[0]
            log.info("Autoselected column %s=%s",
                     self.KEY_IDCOL, self.cfg[self.KEY_IDCOL])

        self._idcol = self.cfg[self.KEY_IDCOL]

        self._runs.set_index(self.cfg[self.KEY_IDCOL],
                             drop=False, inplace=True)

    @property
    def idcol(self):
        if self._idcol is None:
            self.choose_id_column()
        return self._idcol

    @property
    def source_cfg(self):
        if self._source_cfg is None:
            self._source_cfg = self.choose_fq_columns()
        return self._source_cfg

    def choose_fq_columns(self):
        """
        Configures the columns referencing the fastq sources
        """
        import pandas as pd

        # get only columns containing string data
        string_cols = self.run_data.select_dtypes(include=['object'])
        # turn NaN into '' so they don't bother us later
        string_cols.fillna('', inplace=True)

        # if barcode column specified, omit that
        if self.KEY_BCCOL in self.cfg:
            string_cols.drop([self.cfg[self.KEY_BCCOL]], axis=1, inplace=True)

        # if read columns specified, constrain to those
        if self.KEY_READCOLS in self.cfg:
            read_cols = self.cfg[self.KEY_READCOLS]
            if isinstance(read_cols, str):
                read_cols = [read_cols]
            try:
                string_cols = string_cols[read_cols]
            except KeyError as e:
                raise YmpConfigError("{}={} references invalid columns: {}"
                                     "".format(self.KEY_READCOLS,
                                               read_cols,
                                               e.args))

        # select type to use for each row
        source_cfg = pd.DataFrame(index=self.runs,
                                  columns=['type', 'r1', 'r2'])

        # prepare array indicating which columns to use for each
        # row, and what type the row source data is
        for pat, nmax, msg, func in (
                (self.RE_FILE, 2, "fastq files", "file"),
                (self.RE_FILE, 1, "fastq files", "file"),
                (self.RE_REMOTE, 2, "remote URLs", "remote"),
                (self.RE_REMOTE, 1, "remote URLs", "remote"),
                (self.RE_SRR, 1, "SRR numbers", "srr")):
            # collect rows not yet assigned values
            no_type_yet = string_cols[source_cfg['type'].isnull()]
            # match the regex to each value
            match = no_type_yet.apply(lambda x: x.str.contains(pat))
            # check if we have more values than allowed for that
            # data source type
            broken_rows = match.sum(axis=1) > nmax
            if any(broken_rows):
                rows = list(self.runs[broken_rows])
                cols = list(self.run_data.columns[match[broken_rows].any])
                raise YmpConfigError(
                    "Some rows contain more than two {}. "
                    "Use {} to specify the desired rows. "
                    "Rows in question: {} "
                    "Columns in question: {} "
                    "".format(msg, self.KEY_READCOLS, rows, cols))
            # collect rows with matched data
            good_rows = match.sum(axis=1).eq(nmax)
            # prepare output matrix
            out = match[good_rows]
            out = out.apply(lambda x: (func,) + tuple(match.columns[x]),
                            axis=1)
            outm = out.apply(pd.Series, index=source_cfg.columns[0:nmax+1])
            source_cfg.update(outm, overwrite=False)

        return source_cfg

    def FQpath(self, run, pair, nosplit=False):
        """Get path for FQ file for `run` and `pair`
        """
        try:
            source = list(self.source_cfg.loc[run])
        except KeyError:
            raise YmpConfigError("No run '{}' in source config".format(run))

        if isinstance(pair, str):
            pair = self.cfgmgr.pairnames.index(pair)

        if self.KEY_BCCOL in self.cfg and not nosplit:
            bccol = self.cfg[self.KEY_BCCOL]
            barcode_file = self.run_data.loc[run][bccol]
            if len(barcode_file) > 0:
                barcode_id = barcode_file.replace("_", "__").replace("/", "_%")
                return (
                    "{project}.split_libraries/{barcodes}/{run}.{pair}.fq.gz"
                    "".format(
                        project=self.project,
                        barcodes=barcode_id,
                        run=run,
                        pair=self.cfgmgr.pairnames[pair])
                )

        kind = source[0]
        if kind == 'srr':
            srr = self.run_data.loc[run][source[1]]
            f = os.path.join(self.cfgmgr.dir.scratch,
                             "SRR",
                             "{}_{}.fastq.gz".format(srr, pair+1))
            return f

        fq_col = source[pair+1]
        if not isinstance(fq_col, str):
            return (
                "Configuration Error: no source for sample {} and read {} "
                "found.".format(run, pair+1))

        fn = self.run_data.loc[run][fq_col]
        if kind == 'file':
            return fn

        if kind == 'remote':
            return make_local_path(self.cfgmgr, fn)

        raise YmpConfigError(
            "Configuration Error: no source for sample {} and read {} found."
            "".format(run, pair+1))

    def unsplit_path(self, barcode_id, pairname):
        barcode_file = barcode_id.replace("_%", "/").replace("__", "_")
        pair = self.cfgmgr.pairnames.index(pairname)

        bccol_name = self.cfg[self.KEY_BCCOL]
        rows = self.run_data[bccol_name] == barcode_file

        # make sure all rows for this have the same source file
        source_cols = self.source_cfg.loc[rows].apply(set)
        if max(source_cols.apply(len)) > 1:
            raise YmpConfigError("Mixed barcode and read files:\n"
                                 + source_cols.to_string())

        return [barcode_file, self.FQpath(rows.index[0], pair, nosplit=True)]

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
            return (isinstance(self.source_cfg.loc[run][pair+1], str)
                    or self.source_cfg.loc[run][0] == 'srr')

        return [
            "{}.{}".format(run, self.cfgmgr.pairnames[pair])
            for run in self.runs
            for pair in pairs
            if have_file(run, pair)
            if not check_rev or have_file(run, 1) == only_pe
        ]

    @property
    def fq_names(self):
        "Names of all FastQ files"
        return self.get_fq_names()

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


def load_projects(cfgmgr, cfg):
    if not cfg:
        return {}
    projects = {name: Project(cfgmgr, name, data)
                for name, data in cfg.items()}
    return projects
