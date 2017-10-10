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

CONTEXT_SETTINGS = {
    'help_option_names': ['-h', '--help']
}

class YmpException(Exception):
    """
    Generic exception raied by YMP
    """
    pass


class YmpConfigNotFound(YmpException):
    """
    Exception raised by YMP if no config was found in current path
    """
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
    """Default parameters for subcommands launching Snakemake"""
    @click.argument("targets", nargs=-1, metavar="FILES")
    @click.option(
        "--dryrun", "-n", default=False, is_flag=True,
        help="Only show what would be done; don't actually run any commands"
    )
    @click.option(
        "--printshellcmds", "-p", default=False, is_flag=True,
        help="Print shell commands to be executed on shell"
    )
    @click.option(
        "--keepgoing", "-k", default=False, is_flag=True,
        help="Keep going as far as possible after individual stages failed"
    )
    @click.option(
        "--verbose", "-v", default=False, is_flag=True,
        help="Increase verbosity. May be given multiple times"
    )
    @click.option(
        "--lock/--no-lock",
        help="Use/don't use locking to prevent clobbering of files"
        " by parallel instances of YMP running"
    )
    @click.option(
        "--rerun-incomplete", "--ri", 'force_incomplete', is_flag=True,
        help="Re-run stages left incomplete in last run"
    )
    @click.option(
        "--forceall", "-F", is_flag=True, default=False,
        help="Force rebuilding of all stages leading to target"
    )
    @click.option(
        "--force", "-f", "forcetargets", is_flag=True, default=False,
        help="Force rebuilding of target"
    )
    @click.option(
        "--conda-prefix", default=os.path.expanduser("~/.ymp/conda"),
        help="Override path to conda environments"
    )
    @click.option(
        "--timestamp", "-T", is_flag=True, default=False,
        help="Add timestamp to logs")
    @click.option(
        "--notemp", is_flag=True, default=False,
        help="Do not remove temporary files"
    )
    @functools.wraps(func)
    def decorated(*args, **kwargs):
        return func(*args, **kwargs)
    return decorated


def start_snakemake(**kwargs):
    """Execute Snakemake with given parameters and targets

    Fixes paths of kwargs['targets'] to be relative to YMP root.
    """
    kwargs['workdir'], prefix = find_root()
    kwargs['use_conda'] = True
    if 'targets' in kwargs:
        kwargs['targets'] = [os.path.join(prefix, t)
                             for t in kwargs['targets']]
    return snakemake.snakemake(
        resource_filename("ymp", "rules/Snakefile"),
        **kwargs)


@click.group()
@click.version_option(version=ymp.__release__)
def cli():
    """
    Welcome to YMP!

    Please find the full manual at https://ymp.readthedocs.io
    """
    pass



@cli.command(context_settings=CONTEXT_SETTINGS)
@snake_params
@click.option("--cores", "-j", default=1)
@click.option("--dag", "printdag", default=False, is_flag=True)
@click.option("--rulegraph", "printrulegraph", default=False, is_flag=True)
@click.option("--debug-dag", default=False, is_flag=True)
@click.option("--debug", default=False, is_flag=True)
def make(**kwargs):
    "Build target(s) locally"
    rval = start_snakemake(**kwargs)
    if not rval:
        sys.exit(1)


@cli.command(context_settings=CONTEXT_SETTINGS)
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
@click.option("--max-cores", "-j")
@click.option("--local-cores", default=16)
@click.option("--args", "qsub_args")
@click.option("--scriptname")
@click.option("--extra-args", "-e")
def submit(profile, extra_args, **kwargs):
    "Build target(s) on cluster"
    # start with default
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
                            ('max_cores', 'nodes')):
        cfg[kw_arg] = cfg[cfg_arg]
        del cfg[cfg_arg]

    rval = start_snakemake(**cfg)
    if not rval:
        sys.exit(1)


@cli.group()
def env():
    """Manipulate conda software environments

    These commands allow accessing the conda software environments managed
    by YMP. Use e.g.

    >>> $(ymp env activate multiqc)

    to enter the software environment for ``multiqc``.
    """
    pass


@env.command(context_settings=CONTEXT_SETTINGS)
@snake_params
def prepare(**kwargs):
    "Create conda environments"
    rval = start_snakemake(create_envs_only=True, **kwargs)
    if not rval:
        sys.exit(1)

@env.command(context_settings=CONTEXT_SETTINGS)
@click.option(
    "--all", "-a", "param_all", is_flag=True,
    help="List all environments, including outdated ones.")
def list(param_all):
    """List conda environments"""
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


@env.command(context_settings=CONTEXT_SETTINGS)
@click.argument("ENVNAME", nargs=-1)
def install(envname):
    "Install conda software environments"
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


@env.command(context_settings=CONTEXT_SETTINGS)
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


@env.command(context_settings=CONTEXT_SETTINGS)
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


@env.command(context_settings=CONTEXT_SETTINGS)
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


@env.command(context_settings=CONTEXT_SETTINGS)
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
