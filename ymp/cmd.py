#!/usr/bin/env python3

import click
from pkg_resources import resource_filename
import snakemake
import os
import sys
import logging
import functools

from ymp.config import icfg
icfg.init()

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
    return (curpath, prefix)


def snake_params(func):
    @click.argument("targets", nargs=-1, metavar="FILES")
    @click.option("--dryrun", "-n", default=False, is_flag=True)
    @click.option("--printshellcmds", "-p", default=False, is_flag=True)
    @click.option("--keepgoing", "-k", default=False, is_flag=True)
    @click.option("--verbose", "-v", default=False, is_flag=True)
    @click.option("--use-conda/--skip-conda", default=True)
    @click.option("--lock/--no-lock")
    @click.option("--rerun-incomplete", "--ri", 'force_incomplete',
                  is_flag=True)
    @click.option("--latency-wait", "-w", default=0)
    @click.option("--forceall", "-F", is_flag=True, default=False)
    @click.option("--force", "-f", "forcetargets", is_flag=True, default=False)
    @click.option("--conda-prefix", default=os.path.expanduser("~/.ymp/conda"))
    @click.option("--timestamp", "-T", is_flag=True, default=False)
    @functools.wraps(func)
    def decorated(*args, **kwargs):
        return func(*args, **kwargs)
    return decorated


@click.group()
def cli():
    pass


@cli.command()
@snake_params
@click.option("--debug", default=False, is_flag=True)
def prepare(**kwargs):
    "create conda environments"
    rval = start_snakemake(create_envs_only=True, **kwargs)
    if not rval:
        sys.exit(1)


@cli.command()
@snake_params
@click.option("--cores", "-j", default=1)
@click.option("--dag", "printdag", default=False, is_flag=True)
@click.option("--rulegraph", "printrulegraph", default=False, is_flag=True)
@click.option("--debug-dag", default=False, is_flag=True)
@click.option("--debug", default=False, is_flag=True)
def make(**kwargs):
    "build target locally"
    rval = start_snakemake(**kwargs)
    if not rval:
        sys.exit(1)


@cli.command()
@snake_params
@click.option("--cores", "-j", "nodes", default=1024)
@click.option("--local-cores", default=8)
@click.option("--cluster-config", "-u", default="cluster.yaml")
@click.option("--jobname", "--jn", "jobname",
              default="ymp.{rulename}.{jobid}.sh")
@click.option("--drmaa-log-dir", default=icfg.dir.log)
def submit(**kwargs):
    "build target on cluster"
    if not os.path.exists(kwargs['drmaa_log_dir']):
        log.warning("Creating directory '%s'", kwargs['drmaa_log_dir'])
        os.mkdir(kwargs['drmaa_log_dir'])

    drmaa = " ".join([
        '-l nodes=1:ppn={threads}',
        '-j oe',
        '-M elmar.pruesse@ucdenver.edu',
        '-l walltime={cluster.walltime}',
        '-l mem={cluster.mem}',
        '-q {cluster.queue}'
    ])
    rval = start_snakemake(drmaa=drmaa, **kwargs)
    if not rval:
        sys.exit(1)


def start_snakemake(**kwargs):
    kwargs['workdir'], prefix = find_root()
    if 'targets' in kwargs:
        kwargs['targets'] = [os.path.join(prefix, t)
                             for t in kwargs['targets']]
    # kwargs['cluster_config'] = os.path.join(kwargs['cluster_config'],
    #                                         kwargs['workdir'])
    # log.warning("Snakemake Params: {}".format(kwargs))
    return snakemake.snakemake(
        resource_filename("ymp", "rules/Snakefile"),
        **kwargs)
