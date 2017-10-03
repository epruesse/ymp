#!/usr/bin/env python3

import click
from pkg_resources import resource_filename
import snakemake
import os
import shutil
import sys
import logging
import functools
import glob

import ymp
from ymp.common import update_dict
from ymp.util import AttrDict
from ymp.config import icfg

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
    @click.option("--lock/--no-lock")
    @click.option("--rerun-incomplete", "--ri", 'force_incomplete',
                  is_flag=True)
    @click.option("--forceall", "-F", is_flag=True, default=False)
    @click.option("--force", "-f", "forcetargets", is_flag=True, default=False)
    @click.option("--conda-prefix", default=os.path.expanduser("~/.ymp/conda"))
    @click.option("--timestamp", "-T", is_flag=True, default=False)
    @click.option("--notemp", is_flag=True, default=False)
    @functools.wraps(func)
    def decorated(*args, **kwargs):
        return func(*args, **kwargs)
    return decorated


def start_snakemake(**kwargs):
    kwargs['workdir'], prefix = find_root()
    kwargs['use_conda'] = True
    if 'targets' in kwargs:
        kwargs['targets'] = [os.path.join(prefix, t)
                             for t in kwargs['targets']]
    return snakemake.snakemake(
        resource_filename("ymp", "rules/Snakefile"),
        **kwargs)


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
@click.option("--profile", "-P", default="default")
@click.option("--config", "-c")
@click.option("--drmaa", "-d", "use_drmaa", is_flag=True)
@click.option("--sync", "-s", "qsub_sync", is_flag=True)
@click.option("--immediate", "-i", is_flag=True)
@click.option("--command", "qsub_cmd")
@click.option("--wrapper", "-w")
@click.option("--max-jobs-per-second", "-m")
@click.option("--latency-wait", "-l")
@click.option("--max-cores", "-j", "cores")
@click.option("--local-cores", default=16)
@click.option("--args", "qsub_args")
@click.option("--scriptname")
@click.option("--extra-args", "-e")
def submit(profile, extra_args, **kwargs):
    # start with default
    print(icfg.cluster.profiles['default'])
    cfg = icfg.cluster.profiles.default
    # select default profile (from cmd or cfg)
    if profile != "default":
        update_dict(cfg, icfg.cluster[profile])
    elif icfg.cluster.profile != "default":
        update_dict(cfg, icfg.cluster.profiles[icfg.cluster.profile])

    # udpate with not-none command line options
    update_dict(cfg, {key: value
                      for key, value in kwargs.items()
                      if value is not None})

    # turn into attrdict
    cfg = AttrDict(cfg)

    # prepare cluster command
    if cfg.use_drmaa:
        param = 'drmaa'
        cfg.qsub_args = [''] + cfg.qsub_args
    elif cfg.qsub_sync:
        param = 'cluster_sync'
        cfg.qsub_args = [cfg.qsub_cmd, cfg.qsub_sync_arg] + cfg.qsub_args
    else:
        param = 'cluster'
        cfg.qsub_args = [cfg.qsub_cmd] + cfg.qsub_args
    # add extra-args
    for ea in (icfg.cluster.extra_args, extra_args):
        if ea is not None:
            if isinstance(ea, str):
                ea = [ea]
            cfg.qsub_args += ea
    # write to snakemake
    cfg[param] = icfg.expand(" ".join(cfg.qsub_args))
    # clean used args
    for arg in ('use_drmaa', 'qsub_sync', 'qsub_sync_arg',
                'qsub_cmd', 'qsub_args'):
        del cfg[arg]

    # rename ymp params to snakemake params
    for cfg_arg, kw_arg in (('immediate', 'immediate_submit'),
                            ('wrapper', 'jobscript'),
                            ('scriptname', 'jobname'),
                            ('max_cores', 'cores')):
        cfg[kw_arg] = cfg[cfg_arg]
        del cfg[cfg_arg]

    rval = start_snakemake(**cfg)
    if not rval:
        sys.exit(1)


@cli.group()
def env():
    "Manipulate conda environments"
    pass


@env.command()
@click.option("--all", "-a", "param_all", is_flag=True, help="List all environments")
def list(param_all):
    "List conda environments"
    width = max((len(env) for env in ymp.envs))+1
    for env in sorted(ymp.envs.values()):
        print("{name:<{width}} {path}".format(
            name=env.name+":",
            width=width,
            path=env.path))
    if param_all:
        for envhash, path in sorted(ymp.envs_dead.items()):
            print("{name:<{width}} {path}".format(
                name=envhash+":",
                width=width,
                path=path))


@env.command()
@click.argument("ENVNAME", nargs=-1)
def create(envname):
    "Create conda environments"
    fail = False

    if len(envname) == 0:
        envnames = ymp.envs.keys()
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
        envnames = ymp.envs.keys()
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
@click.option("--all", "-a", "param_all", is_flag=True, help="Delete all environments")
def clean(param_all):
    "Remove unused conda environments"
    if param_all: # remove up-to-date environments
        for env in ymp.envs.values():
            log.warning("Removing %s (%s)", env.name, env.path)
            shutil.rmtree(env.path)

    # remove outdated environments
    for _, path in ymp.envs_dead.items():
        log.warning("Removing %s", path)
        shutil.rmtree(path)


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
