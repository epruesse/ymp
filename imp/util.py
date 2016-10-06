#from snakemake.io import expand 
from snakemake.workflow import Workflow as _Workflow
from snakemake.io import expand
from snakemake.utils import format
from string import Formatter
import re, os, csv
import textwrap
from subprocess import check_output

def get_ncbi_root():
    root = check_output("""
    module load sratoolkit
    vdb-config /repository/user/main/public/root/
    """, shell=True)
#    root = re.sub("</?root>", "", root).trim()
    return root


def dir2targets(wildcards):
    dirname = wildcards.dir
    colnames = list(config['pe_sample'][next(iter(config['pe_sample']))])
    regex = r"\.by_({})(?:[./]|$)".format("|".join(colnames))
    groups = re.findall(regex, dirname)
    if len(groups) == 0:
        colname='ID'
    else:
        colname=groups[-1]
    targets = set([config['pe_sample'][sample][colname]
            for sample in config['pe_sample']])
    return list(targets)

def dir2targets2(template):
    return lambda wc: expand(template, sample=dir2targets(wc), **wc)


def read_propfiles(files):
    if isinstance(files, str):
        files=[files]
    props = {}
    for file in files:
        with open(file, "r") as f:
            props.update(
                {key: int(float(value))
                     for line in f
                     for key, value in [line.strip().split(maxsplit=1)]}
            )
    return props


def parse_mapfile(config, mapcfg):
    mapfile_basedir=os.path.dirname(mapcfg['file'])
    with open(mapcfg['file']) as f:
        # sniff CSV type
        dialect = csv.Sniffer().sniff(f.read(10240))
        f.seek(0)
        reader = csv.DictReader(f, dialect=dialect)

        # rewrite FQ column names to standard
        reader.fieldnames = [ config['pairnames'][mapcfg['fq_cols'].index(field)]
                              if field in mapcfg['fq_cols'] else field
                              for field in reader.fieldnames ]

        for row in reader:
            # prepend directory to filenames
            for field in row:
                if field in config['pairnames']:
                    row[field] = os.path.join(mapfile_basedir, row[field])
            id = row[mapcfg['name_col']]
            config['pe_samples'].append(id)
            config['pe_sample'][id] = row
                

def parse_mapfiles(config):
    if 'pe_samples' not in config:
        config['pe_samples'] = []
    if 'pe_sample' not in config:
        config['pe_sample'] = {}
    for mapcfg in config['mapfiles']:
        parse_mapfile(config, config['mapfiles'][mapcfg])


_expand_regex = re.compile(
    r"""
    \{([^{}]+)\}
    """, re.VERBOSE)
#    \{([].,\+[\w$]+)\}

class Workflow(_Workflow):
    formatter = Formatter()
    
    def configfile(self, file):
        super().configfile(file)
        global config
        parse_mapfiles(config)

    def _expand_str(self, string):
        keys = []
        values = {}
        
        def ex(match, keys=keys, values=values):
            key = match.group(1)
            
            if key[0] != "$":
                return "{{" + key + "}}"
            key = key[1:]

            if key in keys:
                return "{{_{}}}".format(keys.index(key))
                
            try:
                val = self.formatter.get_field(key, [], self.globals)[0]
                if isinstance(val, str) or not hasattr(val, "__iter__"):
                    return val
                values["_{}".format(len(keys))] = val
                keys += [key]
            except KeyError:
                print("key error" +  key)
                return "{{" + match.group() + "}}"

            return "{{_{}}}".format(keys.index(key))

        a = _expand_regex.sub(ex, string)
#        print(a)
        return expand(a, **values)
        
        
    def _expand(self, item):
        if isinstance(item, str):
            item = self._expand_str(item)
        elif hasattr(item, '__call__'):  # function
            pass
        elif isinstance(item, int):
            pass
        elif isinstance(item, dict):
            for key, value in item.items():
                item[key] = self._expand(value)
        elif isinstance(item, list):
            for i, subitem in enumerate(item):
                item[i] = self._expand(subitem)
        elif isinstance(item, tuple):
            return (self._expand(subitem) for subitem in item)
        else:
            raise ValueError("unable to expand item '{}'".format(repr(item)))

        return item

    def input(self, *paths, **kwpaths):
        paths = self._expand(paths)
        kwpaths = self._expand(kwpaths)
        return super().input(*paths, **kwpaths)

    def output(self, *paths, **kwpaths):
        paths = self._expand(paths)
        kwpaths = self._expand(kwpaths)
        return super().output(*paths, **kwpaths)


def expander(value):
    print("xxx" + value)
    return value


def snake_extend(workflow_in, config_in):
    global config
    config = config_in
    global workflow
    workflow = workflow_in
    workflow.__class__ = Workflow

def xx():
    def my_configfile(f):
        workflow.orig_configfile(f)
        parse_mapfiles(config)
    workflow.orig_configfile = workflow.configfile  
    workflow.configfile = my_configfile


def glob_wildcards(pattern, files=None):
    from itertools import chain
    from snakemake.io import _wildcard_regex, namedtuple, regex
    import regex as re
    
    """
    Glob the values of the wildcards by matching the given pattern to the filesystem.
    Returns a named tuple with a list of values for each wildcard.
    """
    pattern = os.path.normpath(pattern)
    first_wildcard = re.search("{[^{]", pattern)
    dirname = os.path.dirname(pattern[:first_wildcard.start(
    )]) if first_wildcard else os.path.dirname(pattern)
    if not dirname:
        dirname = "."

    names = [match.group('name')
             for match in _wildcard_regex.finditer(pattern)]
    Wildcards = namedtuple("Wildcards", names)
    wildcards = Wildcards(*[list() for name in names])

    pattern = regex(pattern)
    # work around partial matching bug in python regex module
    # by replacing matches for "\" with "[/\0]" (0x0 can't occur in filenames)
    pattern = re.sub('\\\\/','[/\0]', pattern)
    pattern = re.compile(pattern)

    def walker():
        for dirpath, dirnames, filenames in os.walk(dirname):
            for f in filenames:
                if dirpath != ".":
                    f=os.path.join(dirpath, f)
                yield f
            for i in range(len(dirnames)-1, -1, -1):
                d = dirnames[i]
                if dirpath != ".":
                    d=os.path.join(dirpath, d)
                if not pattern.match(os.path.join(d,""), partial=True):
                    del dirnames[i]
                else:
                    yield d
    
    if files is None:
        files = walker()

    for f in files:
        match = re.match(pattern, os.path.normpath(f))
        if match:
            for name, value in match.groupdict().items():
                getattr(wildcards, name).append(value)
    return wildcards


from snakemake.io import Namedlist
def R(code="", **kwargs):
    """Execute R code

    This function executes the R code given as a string. Additional arguments are injected into
    the R environment. The value of the last R statement is returned. 

    The function requires rpy2 to be installed.

    .. code:: python
        R(input=input)

    Args:
        code (str): R code to be executed
        **kwargs (dict): variables to inject into R globalenv
    Yields:
        value of last R statement
        
    """
    try:
        import rpy2.robjects as robjects
        from rpy2.rlike.container import TaggedList
        from rpy2.rinterface import RNULLType
    except ImportError:
        raise ValueError(
            "Python 3 package rpy2 needs to be installed to use the R function.")
    
    # translate Namedlists into rpy2's TaggedList to have named lists in R
    for key in kwargs:
        value = kwargs[key]
        if isinstance(value, Namedlist):
            kwargs[key] = TaggedList([y for x,y in value.allitems()],
                                     [x for x,y in value.allitems()])

    code = format(textwrap.dedent(code), stepout=2)
    # wrap code in function to preserve clean global env and execute
    rval = robjects.r("function({}){{ {} }}".format(",".join(kwargs), code))(**kwargs)

    # Reduce vectors of length 1 to scalar, as implicit in R.
    if isinstance(rval, RNULLType):
        
        rval = None
    if rval and len(rval) == 1:
        return rval[0]
    return rval
            
