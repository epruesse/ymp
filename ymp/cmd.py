#!/usr/bin/env python3

import click
from pkg_resources import resource_filename
import snakemake
import os
import sys
import logging
import functools
import glob

import ymp
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
@click.option("--local-cores", default=16)
@click.option("--cluster-config", "-u", default="cluster.yaml")  # fixme, relative to ymp.yml
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

@cli.group()
def env():
    "Manipulate conda environments"
    pass


@env.command()
def list():
    "List conda environments"
    width = max((len(env) for env in ymp.envs))+1
    for env in sorted(ymp.envs.values()):
        print("{name:<{width}} {path}".format(
            name=env.name+":",
            width=width,
            path=env.path))


@env.command()
@click.argument("ENVNAME", nargs=-1)
def create(envname):
    "Create conda environments"
    fail = False

    if len(envname) == 0:
        envname = ymp.envs.keys()
        log.warning("Creating all (%i) environments.", len(envname))

    for env in envname:
        if env not in ymp.envs:
            log.error("Environment '%s' unknown", env)
            fail = True
        else:
            ymp.envs[env].create()

    if fail:
        exit(1)


@env.command()
@click.argument("ENVNAMES", nargs=-1)
def update(envnames):
    "Update conda environments"
    fail = False

    if len(envnames) == 0:
        envname = ymp.envs.keys()
        log.warning("Updating all (%i) environments.", len(envnames))

    for envname in envnames:
        if envname not in ymp.envs:
            log.error("Environment '%s' unknown", envname)
            fail = True
        else:
            ret = ymp.envs[envname].update()
            if ret != 0:
                log.error("Updating '%s' failed with return code '%i'",
                          envname, ret)
                fail = True
    if fail:
        exit(1)


@env.command()
@click.argument("ENVNAME", nargs=1)
def activate(envname):
    """
    source activate environment

    Usage:
    $(ymp activate env [ENVNAME])
    """
    if envname not in ymp.envs:
        log.error("Environment '%s' unknown", envname)
        exit(1)
    else:
        print("source activate {}".format(ymp.envs[envname].path))


@env.command()
@click.argument("ENVNAME", nargs=1)
@click.argument("COMMAND", nargs=-1)
def run(envname, command):
    """
    Execute COMMAND with activated environment ENV

    Usage:
    ymp env run <ENV> [--] <COMMAND...>

    (Use the "--" if your command line contains option type parameters
     beginning with - or --)
    """

    if envname not in ymp.envs:
        log.error("Environment '%s' unknown", envname)
    else:
        exit(ymp.envs[envname].run(command))
