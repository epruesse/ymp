from snakemake.workflow import Workflow as _Workflow
from snakemake.io import expand
from string import Formatter
import re, os, csv

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


def snake_extend(workflow_in, config_in):
    global config
    config = config_in
    global workflow
    workflow = workflow_in
    workflow.__class__ = Workflow
