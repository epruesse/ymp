import logging
import os
import shutil
import sys
from fnmatch import fnmatch

import click
from click import echo

import ymp
from ymp.cli.make import snake_params, start_snakemake
from ymp.cli.shared_options import group

log = logging.getLogger(__name__)  # pylint: disable=invalid-name

ENV_COLUMNS = ('name', 'hash', 'path', 'installed')


@group()
def env():
    """Manipulate conda software environments

    These commands allow accessing the conda software environments managed
    by YMP. Use e.g.

    >>> $(ymp env activate multiqc)

    to enter the software environment for ``multiqc``.
    """


@env.command(name="list")
@click.option(
    "--static/--no-static", default=True,
    help="List environments statically defined via env.yml files"
)
@click.option(
    "--dynamic/--no-dynamic", default=True,
    help="List environments defined inline from rule files"
)
@click.option(
    "--all", "-a", "param_all", is_flag=True,
    help="List all environments, including outdated ones."
)
@click.option(
    "--sort", "-s", "sort_col",
    type=click.Choice(ENV_COLUMNS), default=ENV_COLUMNS[0],
    help="Sort by column"
)
@click.option(
    "--reverse", "-r", is_flag=True,
    help="Reverse sort order"
)
def ls(param_all, static, dynamic, sort_col, reverse):
    """List conda environments"""
    from ymp.env import Env

    envs = Env.get_envs(static=static, dynamic=dynamic)

    table_content = sorted(({key: str(getattr(env, key))
                             for key in ENV_COLUMNS}
                            for env in envs),
                           key=lambda row: row[sort_col].upper(),
                           reverse=reverse)

    table_header = [{col: col for col in ENV_COLUMNS}]
    table = table_header + table_content
    widths = {col: max(len(row[col]) for row in table)
              for col in ENV_COLUMNS}

    lines = [" ".join("{!s:<{}}".format(row[col], widths[col])
                      for col in ENV_COLUMNS)
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


def get_envs(envnames):
    from ymp.env import Env
    envs = Env.get_envs()
    if envnames:
        envs = [env for env in envs
                if any(fnmatch(env.name, pat) for pat in envnames)]
    else:
        envs = list(envs)
    return envs


@env.command()
@click.argument("ENVNAMES", nargs=-1)
def install(envnames):
    "Install conda software environments"
    envs = get_envs(envnames)
    log.warning(f"Creating {len(envs)} environments.")
    for env in envs:
        env.create()


@env.command()
@click.argument("ENVNAMES", nargs=-1)
def update(envnames):
    "Update conda environments"
    envs = get_envs(envnames)
    log.warning(f"Updating {len(envs)} environments.")
    for env in get_envs(envnames):
        env.update()


@env.command()
@click.argument("ENVNAMES", nargs=-1)
def remove(envnames):
    "Update conda environments"
    envs = get_envs(envnames)
    log.warning(f"Removing {len(envs)} environments.")
    for env in get_envs(envnames):
        if os.path.exists(env.path):
            log.warning("Removing %s (%s)", env.name, env.path)
            shutil.rmtree(env.path)


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
