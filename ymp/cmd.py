#!/usr/bin/env python3

import click
from pkg_resources import resource_filename
import snakemake
import os
import logging
import functools


log = logging.getLogger(__name__)


class YmpException(Exception):
    pass

class YmpConfigNotFound(YmpException):
    pass



def find_root():
    curpath = os.path.abspath(os.getcwd())
    prefix = ""
    while not os.path.exists(os.path.join(curpath, "ymp.yml")):
        left, right = os.path.split(curpath)
        if curpath == left:
            raise YmpConfigNotFound()
        curpath = left
        prefix = os.path.join(right, prefix)
    log.error("%s %s",curpath,prefix)
    return (curpath, prefix)



def snake_params(func):
    @click.argument("targets", nargs=-1, metavar="FILES")
    @click.option("--dryrun", "-n", default=False, is_flag=True)
    @click.option("--printshellcmds", "-p", default=False, is_flag=True)
    @click.option("--keepgoing", "-k", default=False, is_flag=True)
    @click.option("--verbose", "-v", default=False, is_flag=True)
    @click.option("--use-conda/--skip-conda", default=True)
    @click.option("--lock/--no-lock")
    @click.option("--rerun-incomplete","--ri", 'force_incomplete', is_flag=True)
    @click.option("--latency-wait","-w", default=0)
    @click.option("--forceall", "-F", is_flag=True, default=False)
    @functools.wraps(func)
    def decorated(*args, **kwargs):
        return func(*args, **kwargs)
    return decorated

@click.group()
def cli():
    pass

@cli.command()
@snake_params
@click.option("--cores", "-j", default=1)
@click.option("--dag", "printdag", default=False, is_flag=True)
@click.option("--rulegraph", "printrulegraph", default=False, is_flag=True)
def make(**kwargs):
    "generate target files"
    start_snakemake(**kwargs)


@cli.command()
@snake_params
@click.option("--cores", "-j", "nodes", default=1024)
@click.option("--local-cores", default=8)
@click.option("--cluster-config", "-u", default="cluster.yaml")
def submit(**kwargs):
    "generate target files"
    drmaa = " ".join([
        '-l nodes=1:ppn={threads}',
        '-j oe',
        '-M elmar.pruesse@ucdenver.edu',
        '-l walltime={cluster.walltime}',
        '-l mem={cluster.mem}',
        '-q {cluster.queue}'
    ])
    start_snakemake(drmaa=drmaa, **kwargs)

def start_snakemake(**kwargs):
    kwargs['workdir'], prefix = find_root()
    kwargs['targets'] = [os.path.join(prefix, t) for t in kwargs['targets']]
   # kwargs['cluster_config'] = os.path.join(kwargs['cluster_config'], kwargs['workdir'])
    log.warning("Snakemake Params: {}".format(kwargs))
    snakemake.snakemake(resource_filename("ymp", "rules/Snakefile"), **kwargs)
