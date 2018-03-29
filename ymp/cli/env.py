import logging
import os
import shutil
import sys
from fnmatch import fnmatch

import click
from click import echo

import ymp
from ymp.common import ensure_list
from ymp.cli.make import snake_params, start_snakemake
from ymp.cli.shared_options import group

log = logging.getLogger(__name__)  # pylint: disable=invalid-name

ENV_COLUMNS = ('name', 'hash', 'path', 'installed')


def get_envs(patterns=None):
    """Get environments matching glob pattern

    Args:
      envnames: list of strings to match
    """
    from ymp.env import Env
    envs = Env.get_registry()
    if patterns:
        envs = {env: envs[env] for env in envs
                if any(fnmatch(env, pat)
                       for pat in ensure_list(patterns))}
    else:
        envs = envs
    return envs


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
@click.argument("ENVNAMES", nargs=-1)
def ls(param_all, static, dynamic, sort_col, reverse, envnames):
    """List conda environments"""
    envs = get_envs(envnames)

    table_content = [
        {
            key: str(getattr(env, key))
            for key in ENV_COLUMNS
        }
        for env in envs.values()
    ]
    table_content.sort(key=lambda row: row[sort_col].upper(),
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


@env.command()
@click.argument("ENVNAMES", nargs=-1)
def install(envnames):
    "Install conda software environments"
    envs = get_envs(envnames)
    log.warning(f"Creating {len(envs)} environments.")
    for env in envs.values():
        log.error(env)
        env.create()


@env.command()
@click.argument("ENVNAMES", nargs=-1)
def update(envnames):
    "Update conda environments"
    envs = get_envs(envnames)
    log.warning(f"Updating {len(envs)} environments.")
    for env in get_envs(envnames).values():
        env.update()


@env.command()
@click.argument("ENVNAMES", nargs=-1)
def remove(envnames):
    "Remove conda environments"
    envs = get_envs(envnames)
    log.warning(f"Removing {len(envs)} environments.")
    for env in get_envs(envnames).values():
        if os.path.exists(env.path):
            log.warning("Removing %s (%s)", env.name, env.path)
            shutil.rmtree(env.path)


@env.command()
@click.option("--dest", "-d", type=click.Path(), default=".",
              help="Destination file or directory")
@click.option("--overwrite", "-f", is_flag=True, default=False,
              help="Overwrite existing files")
@click.option("--create", "-c", is_flag=True, default=False,
              help="Create environments not yet installed")
@click.argument("ENVNAMES", nargs=-1)
def export(envnames, dest, overwrite, create):
    "Export conda environments"
    envs = get_envs(envnames)
    log.warning(f"Exporting {len(envs)} environments.")
    if not envs:
        return 1
    if os.path.isdir(dest):
        for env in get_envs(envnames).values():
            fn = os.path.join(dest, env.name + ".yml")
            env.export(fn, create, overwrite)
    else:
        if len(envs) > 1:
            log.error("Cannot export multiple environments to one file")
            return 1
        env[0].export(dest, create, overwrite)


@env.command()
@click.option("--all", "-a", "param_all", is_flag=True,
              help="Delete all environments")
@click.argument("ENVNAMES", nargs=-1)
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
    envs = get_envs(envname)
    if not envs:
        log.critical("Environment '%s' unknown", envname)
        exit(1)

    if len(envs) > 1:
        log.critical("Multiple environments match '%s': %s",
                     envname, envs.keys())
        exit(1)

    env = next(iter(envs.values()))
    if not os.path.exists(env.path):
        log.warning("Environment not yet installed")
        env.create()

    print("source activate {}".format(env.path))


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
