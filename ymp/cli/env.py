import logging
import os
import shutil
import sys

import click
from click import echo

import ymp
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
    from ymp.env import Env

    columns = ('name', 'hash', 'path', 'installed')
    sort_col = 'name'
    envs = []
    envs += Env.get_builtin_static_envs()
    envs += Env.get_builtin_dynamic_envs()

    table_content = sorted(({key: getattr(env, key) for key in columns}
                            for env in envs),
                           key=lambda row: row[sort_col])

    table_header = [{col: col for col in columns}]
    table = table_header + table_content
    widths = {col: max(len(str(row[col])) for row in table)
              for col in columns}

    lines = [" ".join("{!s:<{}}".format(row[col], widths[col])
                      for col in columns)
             for row in table]
    echo("\n".join(lines))


@env.command()
@snake_params
def prepare(**kwargs):
    "Create envs needed to build target"
    kwargs['create_envs_only'] = True
    rval = start_snakemake(kwargs)
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
