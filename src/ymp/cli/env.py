import csv
import logging
import os
import shutil
import sys
from contextlib import ExitStack
from fnmatch import fnmatch

import click
from click import echo

import ymp
from ymp.common import ensure_list
from ymp.cli.make import snake_params, start_snakemake
from ymp.cli.shared_options import group

log = logging.getLogger(__name__)  # pylint: disable=invalid-name

ENV_COLUMNS = ("label", "hash", "address", "installed")
ENV_COLUMNS_ALL = (
    "label",
    "hash",
    "content_hash",
    "address",
    "installed",
    "content_deploy",
    "content_pin",
    "container_img_url",
    "is_containerized",
    "is_named",
    "archive_file",
    "content",
)


def get_envs(patterns=None):
    """Get environments matching glob pattern

    Args:
      envnames: list of strings to match
    """
    from ymp.env import Env

    envs = Env.get_registry()
    if patterns:
        envs = {
            env: envs[env]
            for env in envs
            if any(fnmatch(env, pat) for pat in ensure_list(patterns))
        }
    return envs


def get_env(envname):
    """Get single environment matching glob pattern

    Args:
      envname: environment glob pattern
    """
    envs = get_envs(envname)
    if not envs:
        raise click.UsageError("Environment {} unknown".format(envname))

    if len(envs) > 1:
        raise click.UsageError(
            "Multiple environments match '{}': {}" "".format(envname, envs.keys())
        )

    env = next(iter(envs.values()))
    if not os.path.exists(env.address):
        log.warning("Environment not yet installed")
        env.create()
    return env


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
    "--sort",
    "-s",
    "sort_col",
    type=click.Choice(ENV_COLUMNS),
    default=ENV_COLUMNS[0],
    help="Sort by column",
)
@click.option("--reverse", "-r", is_flag=True, help="Reverse sort order")
@click.option(
    "--installed/--not-installed",
    default=None,
    is_flag=True,
    help="List only installed/not installed environments",
)
@click.option(
    "--extra",
    "-e",
    "extra_fields",
    type=str,
    help="Show additional fields (all: everything)",
)
@click.option("--csv", "write_csv", is_flag=True, help="Output as machine readable CSV")
@click.argument("ENVNAMES", nargs=-1)
def ls(sort_col, reverse, envnames, installed, extra_fields, write_csv):
    """List conda environments"""
    envs = get_envs(envnames)
    if extra_fields is None:
        fields = ENV_COLUMNS
    else:
        extra_fields = extra_fields.split(",")
        if "all" in extra_fields:
            extra_fields = ENV_COLUMNS_ALL
        unknown = " ,".join(
            field for field in extra_fields if field not in ENV_COLUMNS_ALL
        )
        if unknown:
            raise click.UsageError(f"Unknown fields requested: {unknown}")
        fields = [
            field
            for field in ENV_COLUMNS_ALL
            if field in extra_fields or field in ENV_COLUMNS
        ]

    table_content = [
        {key: str(getattr(env, key)) for key in fields} for env in envs.values()
    ]
    table_content.sort(key=lambda row: row[sort_col].upper(), reverse=reverse)
    if installed is not None:
        table_content = [
            row for row in table_content if row["installed"] == str(installed)
        ]

    if write_csv:
        writer = csv.DictWriter(sys.stdout, fields)
        writer.writeheader()
        writer.writerows(table_content)
    else:
        table_header = [{col: col for col in fields}]
        table = table_header + table_content
        widths = {col: max(len(row[col]) for row in table) for col in fields}

        lines = [
            " ".join("{!s:<{}}".format(row[col], widths[col]) for col in fields)
            for row in table
        ]
        echo("\n".join(lines))


@env.command()
@snake_params
@click.option(
    "--reinstall", is_flag=True, help="Delete existing environment and reinstall"
)
@click.option(
    "--no-spec", is_flag=True, help="Don't use conda env spec even if present"
)
@click.option(
    "--no-archive", is_flag=True, help="Delete existing archives before install"
)
@click.option(
    "--fresh",
    is_flag=True,
    help="Create fresh install. Implies reinstall, no-spec and no-archve",
)
def prepare(reinstall, no_spec, no_archive, fresh, **kwargs):
    "Create envs needed to build target"
    kwargs["conda_create_envs_only"] = True
    cfg = ymp.get_config()
    if fresh:
        reinstall = no_spec = no_archive = True
    cfg.conda.create.reinstall = reinstall
    cfg.conda.create.nospec = no_spec
    cfg.conda.create.noarchive = no_archive
    rval = start_snakemake(kwargs, unload=False)
    if not rval:
        sys.exit(1)


@env.command()
@click.option("--conda-prefix", "-p", help="Override location for conda environments")
@click.option("--conda-env-spec", "-e", help="Override conda env specs settings")
@click.option("--dry-run", "-n", is_flag=True, help="Only show what would be done")
@click.option(
    "--reinstall", "-r", is_flag=True, help="Delete existing environment and reinstall"
)
@click.option(
    "--no-spec", is_flag=True, help="Don't use conda env spec even if present"
)
@click.option(
    "--no-archive", is_flag=True, help="Delete existing archives before install"
)
@click.option(
    "--fresh",
    is_flag=True,
    help="Create fresh install. Implies reinstall, no-spec and no-archve",
)
@click.argument("ENVNAMES", nargs=-1)
def install(
    conda_prefix,
    conda_env_spec,
    dry_run,
    reinstall,
    no_spec,
    no_archive,
    fresh,
    envnames,
):
    "Install conda software environments"
    if conda_env_spec is not None:
        cfg = ymp.get_config()
        cfg.conda.env_specs = conda_env_spec
    if fresh:
        reinstall = no_spec = no_archive = True

    envs = get_envs(envnames)
    need_install = len([env for env in envs.values() if not env.installed])
    if not reinstall and len(envs) != need_install:
        log.warning(
            "Creating %i environments (%i already installed)",
            need_install,
            len(envs) - need_install,
        )
    else:
        log.warning(f"Creating {len(envs)} environments.")
    for env in envs.values():
        if not reinstall and env.installed:
            continue
        if conda_prefix:
            env.set_prefix(conda_prefix)
        env.create(dry_run, reinstall=reinstall, nospec=no_spec, noarchive=no_archive)


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
        if os.path.exists(env.address):
            log.warning("Removing %s (%s)", env._ymp_name, env.address)
            shutil.rmtree(env.address)


@env.command()
@click.option(
    "--dest",
    "-d",
    type=click.Path(),
    metavar="FILE",
    help="Destination file or directory. If a directory, file names"
    " will be derived from environment names and selected export "
    "format. Default: print to standard output.",
)
@click.option(
    "--overwrite", "-f", is_flag=True, default=False, help="Overwrite existing files"
)
@click.option(
    "--create-missing",
    "-c",
    is_flag=True,
    default=False,
    help="Create environments not yet installed",
)
@click.option(
    "--skip-missing",
    "-s",
    is_flag=True,
    default=False,
    help="Skip environments not yet installed",
)
@click.option(
    "--filetype",
    "-t",
    type=click.Choice(["yml", "txt"]),
    help="Select export format. " "Default: yml unless FILE ends in '.txt'",
)
@click.argument("ENVNAMES", nargs=-1)
def export(envnames, dest, overwrite, create_missing, skip_missing, filetype):
    """Export conda environments

    Resolved package specifications for the selected conda
    environments can be exported either in YAML format suitable for
    use with ``conda env create -f FILE`` or in TXT format containing
    a list of URLs suitable for use with ``conda create --file
    FILE``. Please note that the TXT format is platform specific.

    If other formats are desired, use ``ymp env list`` to view the
    environments' installation path ("prefix" in conda lingo) and
    export the specification with the ``conda`` command line utlity
    directly.

    \b
    Note:
      Environments must be installed before they can be exported. This is due
      to limitations of the conda utilities.  Use the "--create" flag to
      automatically install missing environments.
    """

    envs = get_envs(envnames)

    if skip_missing and create_missing:
        raise click.UsageError(
            "--skip-missing and --create-missing are mutually exclusive"
        )

    if dest and not filetype and dest.endswith(".txt"):
        filetype = "txt"
    if not filetype:
        filetype = "yml"

    missing = [env for env in envs.values() if not env.installed]
    if skip_missing:
        envs = {name: env for name, env in envs.items() if env not in missing}
    elif create_missing:
        log.warning(f"Creating {len(missing)} missing environments...")
        for n, env in enumerate(missing):
            log.warning(f"Step {n+1}/{len(missing)}:")
            env.create()
    else:
        if missing:
            raise click.UsageError(
                f"Cannot export uninstalled environment(s): "
                f"{', '.join(env._ymp_name for env in missing)}.\n"
                f"Use '-s' to skip these or '-c' to create them prior to export."
            )

    if not envs:
        if envnames and not missing:
            log.warning("Nothing to export. No environments matched pattern(s)")
        else:
            log.warning("Nothing to export")
        return

    log.warning(f"Exporting {len(envs)} environments...")

    if dest:
        if os.path.isdir(dest):
            file_names = [
                os.path.join(dest, ".".join((name, filetype))) for name in envs.keys()
            ]
        else:
            file_names = [dest]

        for fname in file_names:
            if not overwrite and os.path.exists(fname):
                raise click.UsageError(f"File '{fname}' exists. Use '-f' to overwrite")

        with ExitStack() as stack:
            files = [stack.enter_context(open(fname, "w")) for fname in file_names]
            files_stack = stack.pop_all()
    else:
        files = [sys.stdout]
        files_stack = ExitStack()
        file_names = ["stdout"]

    sep = False
    if len(files) == 1:
        sep = True
        files *= len(envs)
        file_names *= len(envs)

    with files_stack:
        generator = enumerate(zip(envs.values(), files, file_names))
        n, (env, fd, name) = next(generator)
        log.warning(f"Step {n+1}/{len(envs)}: writing to {name}")
        env.export(fd, typ=filetype)
        for n, (env, fd, name) in generator:
            log.warning(f"Step {n+1}/{len(envs)}: writing to {name}")
            if sep:
                fd.write("---\n")
            env.export(fd, typ=filetype)


@env.command()
@click.option("--all", "-a", "param_all", is_flag=True, help="Delete all environments")
@click.argument("ENVNAMES", nargs=-1)
def clean(param_all):
    "Remove unused conda environments"
    if param_all:  # remove up-to-date environments
        for env in ymp.env.by_name.values():
            if os.path.exists(env.address):
                log.warning("Removing %s (%s)", env._ymp_name, env.address)
                shutil.rmtree(env.address)

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
    env = get_env(envname)
    print("source activate {}".format(env.address))


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
    env = get_env(envname)
    sys.exit(env.run(command))
