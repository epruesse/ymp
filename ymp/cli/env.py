import logging
import os
import shutil
import sys

import click

import ymp
import ymp.env
from ymp.cli.make import snake_params, start_snakemake
from ymp.cli.shared_options import group

log = logging.getLogger(__name__)


@group()
def env():
    """Manipulate conda software environments

    These commands allow accessing the conda software environments managed
    by YMP. Use e.g.

    >>> $(ymp env activate multiqc)

    to enter the software environment for ``multiqc``.
    """


@env.command()
@click.option(
    "--all", "-a", "param_all", is_flag=True,
    help="List all environments, including outdated ones."
)
def list(param_all):
    """List conda environments"""
    width = max((len(env) for env in ymp.env.by_name))+1
    for env in sorted(ymp.env.by_name.values()):
        path = env.path
        if not os.path.exists(path):
            path += " (NOT INSTALLED)"
        print("{name:<{width}} {path}".format(
            name=env.name+":",
            width=width,
            path=path))
    if param_all:
        for envhash, path in sorted(ymp.env.dead.items()):
            print("{name:<{width}} {path}".format(
                name=envhash+":",
                width=width,
                path=path))


@env.command()
@snake_params
def prepare(**kwargs):
    "Create conda environments"
    rval = start_snakemake(create_envs_only=True, **kwargs)
    if not rval:
        sys.exit(1)


@env.command()
@click.argument("ENVNAME", nargs=-1)
def install(envname):
    "Install conda software environments"
    fail = False

    if len(envname) == 0:
        envname = ymp.env.by_name.keys()
        log.warning("Creating all (%i) environments.", len(envname))

    for env in envname:
        if env not in ymp.env.by_name:
            log.error("Environment '%s' unknown", env)
            fail = True
        else:
            ymp.env.by_name[env].create()

    if fail:
        exit(1)


@env.command()
@click.argument("ENVNAMES", nargs=-1)
def update(envnames):
    "Update conda environments"
    fail = False

    if len(envnames) == 0:
        envnames = ymp.env.by_name.keys()
        log.warning("Updating all (%i) environments.", len(envnames))

    for envname in envnames:
        if envname not in ymp.env.by_name:
            log.error("Environment '%s' unknown", envname)
            fail = True
        else:
            ret = ymp.env.by_name[envname].update()
            if ret != 0:
                log.error("Updating '%s' failed with return code '%i'",
                          envname, ret)
                fail = True
    if fail:
        exit(1)


@env.command()
@click.option("--all", "-a", "param_all", is_flag=True,
              help="Delete all environments")
def clean(param_all):
    "Remove unused conda environments"
    if param_all:  # remove up-to-date environments
        for env in ymp.env.by_name.values():
            if os.path.exists(env.path):
                log.warning("Removing %s (%s)", env.name, env.path)
                shutil.rmtree(env.path)

    # remove outdated environments
    for _, path in ymp.env.dead.items():
        log.warning("Removing (dead) %s", path)
        shutil.rmtree(path)


@env.command()
@click.argument("ENVNAME", nargs=1)
def activate(envname):
    """
    source activate environment

    Usage:
    $(ymp activate env [ENVNAME])
    """
    if envname not in ymp.env.by_name:
        log.critical("Environment '%s' unknown", envname)
        exit(1)

    env = ymp.env.by_name[envname]
    if not os.path.exists(env.path):
        log.warning("Environment not yet installed")
        env.create()

    print("source activate {}".format(ymp.env.by_name[envname].path))


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

    if envname not in ymp.env.by_name:
        log.critical("Environment '%s' unknown", envname)
        sys.exit(1)

    env = ymp.env.by_name[envname]
    if not os.path.exists(env.path):
        log.warning("Environment not yet installed")
        env.create()

    sys.exit(ymp.env.by_name[envname].run(command))
