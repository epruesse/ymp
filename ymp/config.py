import os
import re
import inspect
from functools import lru_cache
from pkg_resources import resource_filename
import yaml

import logging
log = logging.getLogger(__name__)

from snakemake.io import expand, get_wildcard_names
from snakemake.remote.HTTP import RemoteProvider as HTTPRemoteProvider
HTTP = HTTPRemoteProvider()


import pandas as pd

from ymp.snakemake import ExpandableWorkflow, ColonExpander
from ymp.util import AttrDict
from ymp.common import update_dict


class YmpException(Exception):
    pass

class YmpConfigError(YmpException):
    pass

class YmpConfigNotFound(YmpException):
    pass

class YmpConfigMalformed(YmpException):
    pass

class YmpConfigNoProjects(YmpException):
    pass


def loadData(cfg):
    """Recursively loads csv/tsv type data as defined by yaml structure

    Format:
     - string items are files
     - lists of files are concatenated top to bottom
     - dicts must have one value, 'join' and a two-item list
       the two items are joined 'naturally' on shared headers

    Example:
    - top.csv
    - join:
      - bottom_left.csv
      - bottom_right.csv
    """
    if isinstance(cfg, str):
        return pd.read_csv(cfg, sep=None, engine='python', dtype='str')
    if isinstance(cfg, list):
        return pd.concat(list(map(loadData, cfg)), ignore_index=True)
    if isinstance(cfg, dict):
        if 'join' in cfg:
            tables = list(map(loadData, cfg['join']))
            return pd.merge(*tables)
    raise YmpConfigMalformed()


class Context(object):
    """
    Computes the current groups and group members based on
    the 'context': DatasetConfig and wildcards
    """
    RE_BY = re.compile(r"\.by_([^./]*)(?:[./]|$)")

    def __init__(self, dcfg, wc):
        self.dcfg = dcfg
        self.wc = wc

        df = dcfg._runs

        groupbys=[]
        # extract groupby column from dir or by key, with by having preference
        for key in ['dir', 'by']:
            if hasattr(wc, key):
                groupbys += self.RE_BY.findall(getattr(wc, key))

        if len(groupbys) == 0:
            # no grouping desired
            # fake by grouping with virtual column containing "ALL" as value
            self.groupby = df.groupby(pd.Series("ALL", index=df.index))
        elif groupbys[-1] == "ID":
            # individual grouping desired
            # fake by grouping according to index
            self.groupby = df.groupby(df.index)
        else:
            try:
                self.groupby = dcfg._runs.groupby(groupbys[-1])
            except KeyError:
                raise YmpConfigError("Unkown column in groupby: {}"
                                     "".format(groupbys[-1]))

    @property
    def targets(self):
        """
        Returns the current targets:
         - all "runs" if no by_COLUMN is active
         - the unique values for COLUMN if grouping is active
        """
        return list(self.groupby.indices)

    @property
    def sources(self):
        """
        Returns the runs associated with the current target
        """
        return self.groupby.groups[self.wc.target]


class DatasetConfig(object):
    """Contains configuration for a source dataset to be processed"""
    KEY_DATA = 'data'
    KEY_IDCOL = 'id_col'
    KEY_READCOLS = 'read_cols'

    RE_REMOTE = re.compile(r"^(?:https?|ftp|sftp)://(?:.*)")
    RE_SRR = re.compile(r"^SRR[0-9]+$")
    RE_FILE = re.compile(r"(?:fq|fastq)(?:|\.gz)$")

    def __init__(self, cfg):
        self.cfg = cfg
        self.fieldnames = None
        
        if self.KEY_DATA not in self.cfg:
            raise YmpConfigMalformed("Missing key " + self.KEY_DATA)
        self._runs = loadData(self.cfg[self.KEY_DATA])
        self.choose_id_column()
        self.choose_fq_columns()

    def choose_id_column(self):
        """Configures column to use as index on runs

        If explicitly configured via KEY_IDCOL, verifies that the column
        exists and that it is unique. Otherwise chooses the leftmost
        unique column in the data.
        """
        unique_columns = self._runs.columns[
            self._runs.apply(pd.Series.nunique) == self._runs.shape[0]
        ]
        if len(unique_columns) == 0:
            raise YmpConfigError("No unique columns in project")

        if self.KEY_IDCOL in self.cfg:
            idcol = self.cfg[self.KEY_IDCOL]
            if idcol not in self._runs.columns:
                raise YmpConfigError("Configured column {}={} not found in data. "
                                     "Is the spelling correct?"
                                     "Available columns: {}"
                                     "".format(
                                         self.KEY_IDCOL, idcol, list(self._runs.columns)
                                     ))
            if idcol not in unique_columns:
                raise YmpConfigError("Configured column {}={} is not unique. "
                                     "Unique columns: {}"
                                     "".format(
                                         self.KEY_IDCOL, id_col, list(unique_columns)
                                     ))
        else:
            self.cfg[self.KEY_IDCOL] = unique_columns[0]
            log.info("Autoselected column %s=%s", self.KEY_IDCOL, self.cfg[self.KEY_IDCOL])

        self._runs.set_index(self.cfg[self.KEY_IDCOL], drop=False, inplace=True)

    def choose_fq_columns(self):
        """
        Configures the columns referencing the fastq sources
        """
        # get only columns containing string data
        string_cols = self._runs.select_dtypes(include=['object'])
        # turn NaN into '' so they don't bother us later
        string_cols.fillna('', inplace=True)

        # if read columns specified, constrain to those
        if self.KEY_READCOLS in self.cfg:
            read_cols = self.cfg[self.KEY_READCOLS]
            try:
                string_cols = string_cols[read_cols]
            except KeyError as e:
                raise YmpConfigError("{}={} references invalid columns: {}"
                                     "".format(self.KEY_READCOLS,
                                               read_cols,
                                               e.args))

        # select type to use for each row
        source_cfg = pd.DataFrame(index=self._runs.index,
                                  columns=['type', 'r1', 'r2'])

        for pat, nmax, msg, func in (
                (self.RE_FILE, 2, "fastq files", "file"),
                (self.RE_REMOTE, 2, "remote URLs", "remote"),
                (self.RE_SRR, 1, "SRR numbers", "srr"),
            ):
            no_type_yet = string_cols[source_cfg['type'].isnull()]
            match = no_type_yet.apply(lambda x: x.str.contains(pat))
            broken_rows = match.sum(axis=1) > nmax
            if any(broken_rows):
                rows = list(self._runs.index[broken_rows])
                cols = list(self._runs.columns[match[broken_rows].any])
                raise YmpConfigError("Some rows contain more than two {}. "
                                     "Use {} to specify the desired rows. "
                                     "Rows in question: {} "
                                     "Columns in question: {} "
                                     "".format(msg, self.KEY_READCOLS, rows, cols))
            good_rows = match.sum(axis=1).eq(nmax)
            out = match[good_rows]
            out = out.apply(lambda x: (func,) + tuple(match.columns[x]), axis=1)
            outm = out.apply(pd.Series, index=source_cfg.columns[0:nmax+1])
            source_cfg.update(outm, overwrite=False)

        self._source_cfg = source_cfg

    @property
    def runs(self):
        return self._runs.index

    @property
    def props(self):
        return self._runs

    @lru_cache()
    def get_context(self, wc):
        return Context(self, wc)

    def FQpath(self, run, pair):
        """Get path for FQ file for `run` and `pair`

        Expandable paths are
        SRRnnn: :scratch:/SRR/SRRnnn_:pairnames:.fastq.gz
        fq:     [basedir/]{fn}
        remote: http://...
        """
        try:
            source = list(self._source_cfg.loc[run])
        except KeyError:
            raise YmpException("Internal error")
            
        kind = source[0]
        if kind == 'srr':
            srr = self._runs.loc[run][source[1]]
            f= os.path.join(icfg.scratchdir,
                                "SRR",
                                "{}_{}.fastq.gz".format(srr, pair+1))
            return f
        fn = source[pair+1]
        if kind == 'file':
            if os.path.isabs(fn):
                return fn
            else:
                return os.path.join(self.basedir, fn)

        if kind == 'remote':
            return HTTP.remote(fn, keep_local=True)

        raise YmpException("Internal error: no source for {}:{}"
                           "".format(run, pair))

    @property
    def fastq_basenames(self):
        return ["{}.{}".format(run, icfg.pairnames[pair])
                for run in self.runs
                for pair in range(2)]
        

class SraRunTable(DatasetConfig):
    """Contains dataset configuration specified as a SraRunTable"""
    def __init__(self, cfg):
        super().__init__(cfg)
        if self.cfg['type'] != 'SraRunTable':
            raise self.CantLoad()
        if 'name_col' in self.cfg:
            self.name_col = self.cfg['name_col']
        else:
            self.name_col = "Libary_Name_s"
        
        self.loadRuns(self.name_col)

    def FQpath(self, run, pair):
        return os.path.join(
            icfg.scratchdir,"SRR",
            "{}_{}.fastq.gz".format(self.runs[run]['Run_s'], pair+1)
        )

    @property
    def fastq_basenames(self):
        return ["{}.{}".format(run, icfg.pairnames[pair])
                for run in self.runs
                for pair in range(2)]
    

class Mapfile(DatasetConfig):
    """Contains a dataset configuration specified as a CSV"""
    def __init__(self, cfg):
        super().__init__(cfg)
        if self.cfg['type'] != 'CSV':
            raise self.CantLoad()
        
        self.basedir=os.path.dirname(self.file)
        self.fq_cols = self.cfg['fq_cols']
        self.name_col = self.cfg['name_col']

        self.loadRuns(self.name_col)

    def FQpath(self, run, pair):
        return os.path.join(self.basedir, self.runs[run][self.fq_cols[pair]])

    @property
    def fastq_basenames(self):
        return ["{}.{}".format(run, icfg.pairnames[pair])
                for run in self.runs
                for pair in range(len(self.fq_cols))]


class ConfigExpander(ColonExpander):
    def __init__(self, config_mgr):
        super().__init__()
        self.config_mgr = config_mgr

    class Formatter(ColonExpander.Formatter):
        def _get_column(self, dirname):
            regex = r"\.by_({})(?:[./]|$)".format("|".join(colnames))
            groups = re.findall(regex, dirname)
            if len(groups) == 0:
                return None
            else:
                return groups[-1]
            
        def _get_targets(self, data):
            return set([
                data[item][colname]
                for item in data
                ])

        def get_value(self, field_name, args, kwargs):
            # try to resolve variable as property of the config_mgr
            try:
                return getattr(self.expander.config_mgr, field_name)
            except AttributeError:
                pass

            # try to resolve as part of dataset
            try:
                ds = self.expander.config_mgr.getDatasetFromDir(kwargs['wc'].dir)
                return getattr(ds, field_name)
            except AttributeError:
                pass

            # try to resolve as part of dataset in directory context
            try:
                ct = ds.get_context(kwargs['wc'])
                return getattr(ct, field_name)
            except AttributeError as e:
                pass

            return super().get_value(field_name, args, kwargs)
        

class ConfigMgr(object):
    """Interface to configuration. Singleton as "icfg" """
    KEY_PROJECTS = 'projects'
    CONF_FNAME = 'ymp.yml'

    def __init__(self):
        self.clear()

    def clear(self):
        self._datasets = {}
        self._config = {}
        self._conffiles = []

    def init(self):
        self.clear()
        ExpandableWorkflow.activate()
        self._conffiles += [
            resource_filename("ymp", "/etc/defaults.yml"),
            self.find_config(filename=self.CONF_FNAME)
        ]
        self.load_config()
        self.config_expander = ConfigExpander(self)

    def find_config(self, filename):
        """Locates ymp root directory"""
        if not filename:
            filename = self.CONF_FNAME
        log.debug("Trying to find '%s'", filename)
        curpath = os.path.abspath(os.getcwd())
        log.debug("Checking '%s'", curpath)
        while not os.path.exists(os.path.join(curpath, filename)):
            curpath, removed = os.path.split(curpath)
            log.debug("No; trying '%s'", curpath)
            if removed == "":
                raise YmpConfigNotFound()
        log.debug("Found '%s' in '%s'", filename, curpath)
        return os.path.join(curpath, filename)

    def load_config(self):
        """Loads ymp configuration files"""
        for fn in self._conffiles:
            log.debug("Loading '%s'", fn)
            with open(fn, "r") as f:
                conf = yaml.load(f)
                update_dict(self._config, conf)
        self._datasets = {
            project :  DatasetConfig(self._config[self.KEY_PROJECTS][project])
            for project in self._config[self.KEY_PROJECTS]
        }
        if len(self._datasets) == 0:
            raise YmpConfigNoDatasets()

    def __len__(self):
        "Our length is the number of datasets"
        return length(self._datasets)

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
    def scratchdir(self):
        try:
            return self._config['directories']['scratch']
        except:
            raise KeyError("Missing directories/scratch in config")

    @property
    def scratch(self):
        return self.scratchdir

    @property
    def reportsdir(self):
        try:
            return self._config['directories']['reports']
        except:
            raise KeyError("Missing directories/reports in config")

    @property
    def sra(self):
        try:
            return self._config['directories']['sra']
        except:
            raise KeyError("Missing directories/reports in config")
    
    @property
    def datasets(self):
        """Returns list of all configured datasets"""
        return self._datasets.keys()

    @property
    def db(self):
        return AttrDict(self._config['databases'])

    @property
    def allruns(self):
        return self.getRuns()

    @property
    def allprops(self):
        return self.getProps()

    def getDatasetFromDir(self, dirname):
        try:
            ds = dirname.split(".", 1)[0]
            return self._datasets[ds]
        except:
            raise KeyError("no dataset found matching '{}'".format(dirname))
        
    def expand(self, *args, **kwargs):
        # FIXME:
        res = self.config_expander.expand_input(args, kwargs)[0][0]
        return res
        #return lambda wc: self._expand(template, wc)

    def _expand(self, template, wc=None):
        if wc is None:
            wc={}
        if isinstance(template, str):
            template = [template]
        names = set()
        for item in template:
            names |= get_wildcard_names(item)
 
        sources = [wc]
        try:
            ds = self.getDatasetFromDir(wc.dir)
            sources += [ds]
        except:
            pass
        sources += [self]

        fields = {}
        for name in names:
            for source in sources:
                if name in dir(source):
                    fields[name] = getattr(source, name)
                    break
            if not name in fields:
                fields[name] = "{{{}}}".format(name)

        res = expand(template, **fields)
        return res
            
    def FQpath(self, dataset, run, pairsuff):
        try:
            return self._datasets[dataset].FQpath(run, self.pairnames.index(pairsuff))
        except KeyError:
            return ":::No such file (ds={}, run={}, pair={}):::".format(
                dataset, run, pairsuff)


    def getRuns(self, datasets=None):
        """Returns list of names of Runs of `dataset`, or names of all configured Runs"""
        if not datasets:
            datasets = self.datasets
        if isinstance(datasets, str):
            datasets = [datasets]
        return [
            run
            for dataset in datasets
            for run in self._datasets[dataset].runs
        ]

    def getProps(self, datasets=None):
        """Returns list of properties of `dataset` runs"""
        if not datasets:
            datasets = self.datasets
        if isinstance(datasets, str):
            datasets = [datasets]
        return [
            prop
            for dataset in datasets
            for prop in self._datasets[dataset].props
        ]

icfg = ConfigMgr()
