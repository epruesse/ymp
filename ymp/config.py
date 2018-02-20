import logging

import os
import re

from pkg_resources import resource_filename

import yaml

from ymp.common import parse_number, update_dict
from ymp.exceptions import YmpException
from ymp.snakemake import \
    ColonExpander, \
    CondaPathExpander, \
    DefaultExpander, \
    ExpandableWorkflow, \
    InheritanceExpander, \
    RecursiveExpander
from ymp.stage import StageExpander
from ymp.util import AttrDict

log = logging.getLogger(__name__)


class YmpConfigError(YmpException):
    pass


class YmpConfigNotFound(YmpException):
    pass


class YmpConfigMalformed(YmpException):
    pass


class YmpConfigNoProjects(YmpException):
    pass


class YmpDataParserError(YmpException):
    pass


class MkdirDict(AttrDict):
    "Creates directories as they are requested"
    def __getattr__(self, attr):
        dirname = super().__getattr__(attr)
        if not os.path.exists(dirname):
            log.warning("Creating directory {}".format(dirname))
            os.makedirs(dirname)
        return dirname


def make_path_reference(path):
    url_match = re.match("^(http|https|ftp|ftps)://", path)
    if url_match:
        return os.path.join(
            icfg.dir.downloads,
            url_match.group(1),
            path[url_match.end():]
        )
    return path


def is_fq(path):
    return isinstance(path, str) and (
        path.endswith(".fq.gz")
        or path.endswith(".fastq.gz")
        or path.endswith(".fq")
        or path.endswith(".fastq")
    )


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
                raise YmpDataParserError(
                    "Could not load specified data file '{}'."
                    " If this is an Excel file, you might need"
                    " to install 'xlrd'."
                    "".format(cfg)
                )
        rdir = os.path.dirname(cfg)
        data = data.applymap(
            lambda s: os.path.join(rdir, s)
            if is_fq(s) and os.path.exists(os.path.join(rdir, s))
            else s)
        return data

    if isinstance(cfg, list):
        return pd.concat(list(map(load_data, cfg)), ignore_index=True)
    if isinstance(cfg, dict):
        # JOIN
        if 'join' in cfg:
            tables = list(map(load_data, cfg['join']))
            try:
                return pd.merge(*tables)
            except MergeError as e:
                log.exception("Failed to `join` configured data.\n"
                              "Config Fragment:\n{}\n\n"
                              "Joined table indices:\n{}\n\n"
                              "".format(yaml.dump(cfg),
                                        "\n".join(
                                            [", ".join(table.columns.tolist())
                                             for table in tables]
                                        ))
                              )
                raise YmpDataParserError(e)
        # PASTE
        if 'paste' in cfg:
            tables = list(map(load_data, cfg['paste']))
            manyrow = [table for table in tables if len(table) > 1]
            if len(manyrow) > 0:
                nrows = len(manyrow[0])
                if any(len(table) != nrows for table in manyrow[1:]):
                    raise YmpDataParserError(
                        "Failed to `paste` configured data. "
                        "Row counts differ and are not 1."
                        "Config Fragment:\n{}\n\n"
                        "Row counts: {}\n"
                        "".format(yaml.dump(cfg),
                                  ", ".join((str(len(table))
                                             for table in manyrow))
                                  )
                    )
                tables = [
                    table if len(table) > 1
                    else pd.concat([table]*nrows, ignore_index=True)
                    for table in tables
                ]
            return pd.concat(tables, axis=1)
        # TABLE
        if 'table' in cfg:
            return pd.DataFrame.from_items((
                (key, value.split(','))
                for row in cfg['table']
                for key, value in row.items()
            ))
    raise YmpConfigMalformed()


class Reference(object):
    """
    Represents (remote) reference file/database configuration
    """
    def __init__(self, cfgmgr, reference, cfg):
        self.name = reference
        self.cfgmgr = cfgmgr
        self.cfg = cfg

    def __str__(self):
        res = "{refdir}/{refname}/ALL.contigs".format(
            refdir=self.cfgmgr.dir.references,
            refname=self.name
        )
        return res

    def get_file(self, filename):
        if filename == "ALL.contigs.fasta.gz":
            res = make_path_reference(self.cfg[0]['url'])
            log.error("Reference {} {} = {}".format(self.name, filename, res))
            return res
        return "__no_such_file__"
        raise KeyError("No file '{}' in Reference '{}'"
                       "".format(filename, self.name))

    @property
    def dir(self):
        return os.path.join(self.cfgmgr.dir.references, self.name)


class Context(object):
    """
    Computes available targets from stage stack encoded in directory name

    sources:
    targets:
    target:
    reference:

    Computes the current groups and group members based on
    the 'context': DatasetConfig and wildcards
    """
    RE_BY = re.compile(r"\.by_([^./]*)(?:[./]|$)")

    def __init__(self, dcfg, kwargs):
        self.dcfg = dcfg
        self.kwargs = kwargs
        self.wc = kwargs.get('wc', None)
        self.rule = kwargs.get('rule', None)
        self._group_by = None
        log.debug("new context for {}".format(kwargs))

    @property
    def group_by(self):
        if self._group_by is not None:
            return self._group_by

        df = self.dcfg.run_data
        import pandas as pd

        groupbys = []
        # extract groupby column from dir or by key, with by having preference
        for key in ['_YMP_DIR', 'dir', '_YMP_VRT', 'by']:
            if hasattr(self.wc, key):
                groupbys += self.RE_BY.findall(getattr(self.wc, key))

        if len(groupbys) == 0:
            # no grouping desired
            # fake by grouping with virtual column containing "ALL" as value
            self._group_by = df.groupby(pd.Series("ALL", index=df.index))
        elif groupbys[-1] == "ID":
            # individual grouping desired
            # fake by grouping according to index
            self._group_by = df.groupby(df.index)
        else:
            try:
                self._group_by = df.groupby(groupbys[-1])
            except KeyError:
                raise YmpConfigError("Unkown column in groupby: {}"
                                     "".format(groupbys[-1]))
        return self._group_by

    def __repr__(self):
        return "{}(wc={},groups={})".format(
            self.__class__.__name__,
            list(self.wc.allitems()),
            self.group_by.groups
        )

    @property
    def reference(self):
        """
        Returns the currently selected reference
        """
        references = self.dcfg.cfgmgr.ref.keys()
        re_ref = re.compile(r"\.(ref_(?:{})|assemble_megahit)(?=[./]|$)"
                            r"".format("|".join(references)))
        stackstr = "".join(
            getattr(self.wc, key)
            for key in ['dir', '_YMP_PRJ', '_YMP_DIR', '_YMP_VRT', '_YMP_ASM']
            if hasattr(self.wc, key)
        )
        matches = re_ref.findall(stackstr)

        if not matches:
            raise KeyError("No reference found for {} and {}"
                           "".format(self.rule, self.wc))

        ref_name = matches[-1]
        if ref_name.startswith("ref_"):
            reference = self.dcfg.cfgmgr.ref[ref_name[4:]]
        else:
            target = getattr(self.wc, 'target', 'ALL')
            reference = "{}/{}.contigs".format(stackstr, target)

        log.debug("Reference selected for {}: {}".format(self.rule, reference))

        return reference

    @property
    def targets(self):
        """
        Returns the current targets:

         - all "runs" if no by_COLUMN is active
         - the unique values for COLUMN if grouping is active
        """
        return list(self.group_by.indices)

    @property
    def sources(self):
        """
        Returns the runs associated with the current target
        """
        try:
            target = self.wc.target
        except AttributeError:
            raise YmpException(
                "Using '{:sources:}' requires '{target}' wildcard"
            )

        try:
            sources = self.group_by.groups[target]
        except KeyError:
            log.debug(list(self.wc.allitems()))
            raise YmpException(
                "Target '{}' not available. Possible choices are '{}'"
                "".format(target, list(self.group_by.groups.keys()))
            )
        return sources


class DatasetConfig(object):
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
        self.project = project
        self.cfgmgr = cfgmgr
        self.cfg = cfg
        self.fieldnames = None
        self._runs = None
        self._source_cfg = None

        if self.KEY_DATA not in self.cfg:
            raise YmpConfigMalformed("Missing key " + self.KEY_DATA)

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

        unique_columns = self._runs.columns[
            self._runs.apply(pd.Series.nunique) == self._runs.shape[0]
        ]
        if len(unique_columns) == 0:
            raise YmpConfigError("No unique columns in project")

        if self.KEY_IDCOL in self.cfg:
            idcol = self.cfg[self.KEY_IDCOL]
            if idcol not in self._runs.columns:
                raise YmpConfigError(
                    "Configured column {}={} not found in data. "
                    "Is the spelling correct?"
                    "Available columns: {}"
                    "".format(
                        self.KEY_IDCOL, idcol, list(self._runs.columns)
                    )
                )
            if idcol not in unique_columns:
                raise YmpConfigError(
                    "Configured column {}={} is not unique. "
                    "Unique columns: {}"
                    "".format(
                        self.KEY_IDCOL, idcol, list(unique_columns)
                    )
                )
        else:
            self.cfg[self.KEY_IDCOL] = unique_columns[0]
            log.info("Autoselected column %s=%s",
                     self.KEY_IDCOL, self.cfg[self.KEY_IDCOL])

        self._runs.set_index(self.cfg[self.KEY_IDCOL],
                             drop=False, inplace=True)

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

    # @lru_cache()
    def get_context(self, kwargs):
        return Context(self, kwargs)

    def FQpath(self, run, pair, nosplit=False):
        """Get path for FQ file for `run` and `pair`
        """
        try:
            source = list(self.source_cfg.loc[run])
        except KeyError:
            raise YmpException("Internal error. "
                               "No run '{}' in source config".format(run))

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
            f = os.path.join(icfg.dir.scratch,
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
            return make_path_reference(fn)

        raise YmpException(
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
            raise YmpException("Mixed barcode and read files:\n"
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
            "{}.{}".format(run, icfg.pairnames[pair])
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


class ConfigExpander(ColonExpander):
    def __init__(self, config_mgr):
        super().__init__()
        self.config_mgr = config_mgr

    def expands_field(self, field):
        return field not in 'func'

    class Formatter(ColonExpander.Formatter):
        def get_value(self, field_name, args, kwargs):
            cfg = self.expander.config_mgr

            # try to resolve variable as property of the config_mgr
            res = getattr(cfg, field_name, None)
            if res:
                return res

            ds = "no ds"
            ct = "no ct"
            dirname = "no dir"
            wc = "no wc"
            if "wc" in kwargs:
                wc = kwargs["wc"]

                dirname = getattr(wc, "dir", None)
                if not dirname:
                    dirname = getattr(wc, "_YMP_PRJ", None)
                if dirname:
                    # Called late with "{dir}" in wildcards
                    # try to resolve as part of dataset
                    ds = cfg.getDatasetFromDir(dirname)
                    res = getattr(ds, field_name, None)
                    if res is not None:
                        return res

                    # try to resolve as part of context
                    ct = ds.get_context(kwargs)
                    res = getattr(ct, field_name, None)
                    if res is not None:
                        return res

            return super().get_value(field_name, args, kwargs)


class ConfigMgr(object):
    """Interface to configuration. Singleton as "icfg" """
    KEY_PROJECTS = 'projects'
    KEY_REFERENCES = 'references'
    CONF_FNAME = 'ymp.yml'
    CONF_DEFAULT_FNAME = resource_filename("ymp", "/etc/defaults.yml")
    CONF_USER_FNAME = os.path.expanduser("~/.ymp/ymp.yml")

    def __init__(self):
        self.clear()

    def clear(self):
        self._datasets = {}
        self._config = {}
        self._conffiles = []
        ExpandableWorkflow.clear()

    def init(self):
        self.clear()
        ExpandableWorkflow.activate()
        self.find_config()
        self.load_config()
        self.recursive_expander = RecursiveExpander()
        self.config_expander = ConfigExpander(self)
        self.conda_path_expander = \
            CondaPathExpander(self.search_paths.conda_env)
        self.default_expander = \
            DefaultExpander(params=([], {'mem': self.mem()}))
        self.inheritance_expander = InheritanceExpander()
        self.stage_expander = StageExpander()

    def find_config(self):
        """Locates ymp config files and sets ymp root"""
        # always include defaults
        self._conffiles += [self.CONF_DEFAULT_FNAME]

        # include user config if present
        if os.path.exists(self.CONF_USER_FNAME):
            self._conffiles += [self.CONF_USER_FNAME]

        # try to find an ymp.yml in CWD and upwards
        filename = self.CONF_FNAME
        log.debug("Trying to find '%s'", filename)
        curpath = os.path.abspath(os.getcwd())
        log.debug("Checking '%s'", curpath)
        while not os.path.exists(os.path.join(curpath, filename)):
            curpath, removed = os.path.split(curpath)
            log.debug("No; trying '%s'", curpath)
            if removed == "":
                self._root = None
                return
        log.debug("Found '%s' in '%s'", filename, curpath)
        self._root = curpath
        self._conffiles += [os.path.join(self._root, self.CONF_FNAME)]

    @property
    def root(self):
        return self._root

    def load_config(self):
        """Loads ymp configuration files"""
        for fn in self._conffiles:
            log.debug("Loading '%s'", fn)
            with open(fn, "r") as f:
                conf = yaml.load(f)
                update_dict(self._config, conf)

        projects = self._config.get(self.KEY_PROJECTS, {})
        if not projects:
            projects = {}
        self._datasets = {
            project:  DatasetConfig(self, project, cfg)
            for project, cfg in projects.items()
        }

        references = self._config.get(self.KEY_REFERENCES, {})
        if not references:
            references == {}
        self._references = {
            reference: Reference(self, reference, cfg)
            for reference, cfg in references.items()
        }

        if len(self._datasets) == 0:
            log.warning("No projects found in configuration")

    def __len__(self):
        "Our length is the number of datasets"
        return len(self._datasets)

    def __getitem__(self, key):
        "Returns DatasetConfig"
        return self._datasets[key]

    def __iter__(self):
        "Returns iterator over DatasetConfigs"
        return iter(self._datasets.keys())

    @property
    def pairnames(self):
        return self._config['pairnames']

    @property
    def search_paths(self):
        return AttrDict(self._config['search_paths'])

    @property
    def dir(self):
        """
        Dictionary of relative paths of named YMP directories

        The directory paths are relative to the YMP root workdir.
        """
        return AttrDict(self._config['directories'])

    @property
    def absdir(self):
        """
        Dictionary of absolute paths of named YMP directories

        Directories will be created on the fly as they are requested.
        """
        return MkdirDict({name: os.path.abspath(value)
                         for name, value in self.dir.items()})

    @property
    def cluster(self):
        """
        The YMP cluster configuration.
        """
        return AttrDict(self._config['cluster'])

    @property
    def ref(self):
        return AttrDict(self._references)

    @property
    def datasets(self):
        """
        Names of all configured datasets
        """
        return self._datasets.keys()

    @property
    def limits(self):
        """
        The YMP limits configuration.
        """
        return AttrDict(self._config['limits'])

    @property
    def allruns(self):
        """
        Names of all configured runs
        """
        return self.getRuns()

    @property
    def proj(self):
        """
        Project base directory
        """
        return "{_YMP_PRJ}"


    def getDatasetFromDir(self, dirname):
        try:
            ds = dirname.split(".", 1)[0]
            return self._datasets[ds]
        except KeyError:
            raise KeyError("no dataset found matching '{}'".format(dirname))

    def expand(self, item, **kwargs):
        res = self.config_expander.expand(None, item, kwargs)
        return res

    def getRuns(self, datasets=None):
        """Returns list of names of Runs of `dataset`, or names of all
        configured Runs"""
        if not datasets:
            datasets = self.datasets
        if isinstance(datasets, str):
            datasets = [datasets]
        return [
            run
            for dataset in datasets
            for run in self._datasets[dataset].runs
        ]

    def mem(self, base="0", per_thread=None, unit="m"):
        """Clamp memory to configuration limits

        Params:
           base:       base memory requested
           per_thread: additional mem required per allocated thread
           unit:       output unit (b, k, m, g, t)
        """
        mem = parse_number(base)
        max_mem = parse_number(self.limits.max_mem)
        if mem > max_mem:
            mem = max_mem
        min_mem = parse_number(self.limits.min_mem)
        if mem < min_mem:
            mem = min_mem

        div = parse_number("1"+unit)

        return int(mem / div)


icfg = ConfigMgr()
icfg.init()
