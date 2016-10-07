import csv
import os
import re

from snakemake.io import expand
from snakemake.workflow import config


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
    return sorted(list(targets))


def dir2targets2(template):
    return lambda wc: expand(template, sample=dir2targets(wc), **wc)


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


def parse_mapfiles():
    if 'pe_samples' not in config:
        config['pe_samples'] = []
    if 'pe_sample' not in config:
        config['pe_sample'] = {}
    for mapcfg in config['mapfiles']:
        parse_mapfile(config, config['mapfiles'][mapcfg])
