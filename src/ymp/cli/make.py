"""Implements subcommands for ``ymp make`` and ``ymp submit``"""

import functools
import logging
import os
import shutil
import sys

import click

import ymp
from ymp.cli.shared_options import command, nohup_option
from ymp.common import Cache
from ymp.exceptions import YmpException

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class TargetParam(click.ParamType):
    """Handles tab expansion for build targets"""

    @classmethod
    def complete(cls, _ctx, incomplete):
        """Try to complete incomplete command

        This is executed on tab or tab-tab from the shell

        Args:
          ctx: click context object
          incomplete: last word in command line up until cursor

        Returns:
          list of words incomplete can be completed to
        """

        # errlog = open("err.txt", "a")
        errlog = open("/dev/null", "a")
        errlog.write("\nincomplete={}\n".format(incomplete))
        cache = Cache.get_cache("completion")
        query_stages = incomplete.split(".")
        errlog.write("stages={}\n".format(query_stages))
        options: list = []

        if len(query_stages) == 1:  # expand projects
            cfg = ymp.get_config()
            options = cfg.projects.keys()
        else:  # expand stages
            if 'stages' in cache:
                stages = cache['stages']
            else:
                from ymp.snakemake import load_workflow
                from ymp.stage import Stage
                load_workflow()
                stages = cache['stages'] = list(Stage.get_registry().keys())
            options = stages
        options = [o for o in options if o.startswith(query_stages[-1])]
        prefix = ".".join(query_stages[:-1])
        if prefix:
            prefix += "."
        errlog.write("prefix={}\n".format(prefix))
        options = [prefix + o + cont for o in options for cont in ("/", ".")]
        errlog.write("options={}\n".format(options))
        errlog.close()
        return options


def snake_params(func):
    """Default parameters for subcommands launching Snakemake"""
    @click.argument(
        "targets", nargs=-1, metavar="TARGET_FILES",
        type=TargetParam()
    )
    @click.option(
        "--dryrun", "-n", default=False, is_flag=True,
        help="Only show what would be done"
    )
    @click.option(
        "--printshellcmds", "-p", default=False, is_flag=True,
        help="Print shell commands to be executed on shell"
    )
    @click.option(
        "--keepgoing", "-k", default=False, is_flag=True,
        help="Don't stop after failed job"
    )
    @click.option(
        "--lock/--no-lock",
        help="Use/don't use locking to prevent clobbering of files"
        " by parallel instances of YMP running"
    )
    @click.option(
        "--rerun-incomplete", "--ri", 'force_incomplete', is_flag=True,
        help="Re-run jobs left incomplete in last run"
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
        metavar="PATH",
        help="Override path to conda environments"
    )
    @click.option(
        "--timestamp", "-T", is_flag=True, default=False,
        help="Add timestamp to logs")
    @click.option(
        "--notemp", is_flag=True, default=False,
        help="Do not remove temporary files"
    )
    @nohup_option
    @functools.wraps(func)
    def decorated(*args, **kwargs):  # pylint: disable=missing-docstring
        return func(*args, **kwargs)
    return decorated


class YmpConfigNotFound(YmpException):
    """
    Exception raised by YMP if no config was found in current path
    """
    pass


def start_snakemake(kwargs):
    """Execute Snakemake with given parameters and targets

    Fixes paths of kwargs['targets'] to be relative to YMP root.
    """
    cfg = ymp.get_config()
    root_path = cfg.root
    cur_path = os.path.abspath(os.getcwd())
    if not cur_path.startswith(root_path):
        raise YmpException("internal error - CWD moved out of YMP root?!")
    cur_path = cur_path[len(root_path):]

    kwargs['workdir'] = root_path

    # translate renamed arguments to snakemake synopsis
    arg_map = {
        'immediate': 'immediate_submit',
        'wrapper': 'jobscript',
        'scriptname': 'jobname',
        'cluster_cores': 'nodes',
        'snake_config': 'config',
        'drmaa': None,
        'sync': None,
        'sync_arg': None,
        'command': None,
        'args': None,
        'nohup': None
    }
    kwargs = {arg_map.get(key, key): value
              for key, value in kwargs.items()
              if arg_map.get(key, key) is not None}

    # our debug flag sets a new excepthoook handler, to we use this
    # to decide whether snakemake should run in debug mode
    if sys.excepthook.__module__ != "sys":
        log.warning(
            "Custom excepthook detected. Having Snakemake open stdin "
            "inside of run: blocks")
        kwargs['debug'] = True

    # map our logging level to snakemake logging level
    if log.getEffectiveLevel() > logging.WARNING:
        kwargs['quiet'] = True
    if log.getEffectiveLevel() < logging.WARNING:
        kwargs['verbose'] = True
    kwargs['use_conda'] = True
    if 'targets' in kwargs:
        kwargs['targets'] = [os.path.join(cur_path, t)
                             for t in kwargs['targets']]

    log.debug("Running snakemake.snakemake with args: %s", kwargs)
    import snakemake
    return snakemake.snakemake(ymp._snakefile, **kwargs)


@command()
@snake_params
@click.option(
    "--cores", "-j", default=1, metavar="CORES",
    help="The number of parallel threads used for scheduling jobs"
)
@click.option(
    "--dag", "printdag", default=False, is_flag=True,
    help="Print the Snakemake execution DAG and exit"
)
@click.option(
    "--rulegraph", "printrulegraph", default=False, is_flag=True,
    help="Print the Snakemake rule graph and exit"
)
@click.option(
    "--debug-dag", default=False, is_flag=True,
    help="Show candidates and selections made while the rule execution graph "
    "is being built"
)
@click.option(
    "--debug", default=False, is_flag=True,
    help="Set the Snakemake debug flag"
)
def make(**kwargs):
    "Build target(s) locally"
    rval = start_snakemake(kwargs)
    if not rval:
        sys.exit(1)


@command()
@snake_params
@click.option(
    "--profile", "-P", metavar="NAME",
    help="Select cluster config profile to use. Overrides cluster.profile "
    "setting from config."
)
@click.option(
    "--snake-config", "-c", metavar="FILE",
    help="Provide snakemake cluster config file"
)
@click.option(
    "--drmaa", "-d", is_flag=True,
    help="Use DRMAA to submit jobs to cluster. Note: Make sure you have "
    "a working DRMAA library. Set DRMAA_LIBRAY_PATH if necessary."
)
@click.option(
    "--sync", "-s", is_flag=True,
    help="Use synchronous cluster submission, keeping the submit command "
    "running until the job has completed. Adds qsub_sync_arg to cluster "
    "command"
)
@click.option(
    "--immediate", "-i", is_flag=True,
    help="Use immediate submission, submitting all jobs to the cluster "
    "at once."
)
@click.option(
    "--command", metavar="CMD",
    help="Use CMD to submit job script to the cluster"
)
@click.option(
    "--wrapper", metavar="CMD",
    help="Use CMD as script submitted to the cluster. See Snakemake "
    "documentation for more information."
)
@click.option(
    "--max-jobs-per-second", metavar="N",
    help="Limit the number of jobs submitted per second"
)
@click.option(
    "--latency-wait", "-l", metavar="T",
    help="Time in seconds to wait after job completed until files are "
    "expected to have appeared in local file system view. On NFS, this "
    "time is governed by the acdirmax mount option, which defaults to "
    "60 seconds."
)
@click.option(
    "--cluster-cores", "-J", type=int, metavar="N",
    help="Limit the maximum number of cores used by jobs submitted at a time"
)
@click.option(
    "--cores", "-j", default=16, metavar="N",
    help="Number of local threads to use"
)
@click.option(
    "--args", "args", metavar="ARGS",
    help="Additional arguments passed to cluster submission command. "
    "Note: Make sure the first character of the argument is not '-', "
    "prefix with ' ' as necessary."
)
@click.option(
    "--scriptname", metavar="NAME",
    help="Set the name template used for submitted jobs"
)
def submit(profile, **kwargs):
    """Build target(s) on cluster

    The parameters for cluster execution are drawn from layered profiles. YMP
    includes base profiles for the "torque" and "slurm" cluster engines.

    """
    cfg = ymp.get_config()

    # The cluster config uses profiles, which are assembled by layering
    # the default profile, the selected profile and additional command
    # line parameters. The selected profile is either specified via
    # "
    config = cfg.cluster.profiles.default
    profile_name = profile or cfg.cluster.profile
    if profile_name:
        config.add_layer(profile_name,
                         cfg.cluster.profiles[profile_name])
    cli_params = {key: value
                  for key, value in kwargs.items()
                  if value is not None}
    config.add_layer("<shell arguments>",
                     cli_params)

    # prepare cluster command
    if config.command is None:
        raise click.UsageError("""
        No cluster submission command configured.
        Please check the manual on how to configure YMP for your cluster"
        """)
    cmd = config.command.split() + config.args.values()
    if config.drmaa:
        param = 'drmaa'
        cmd[0] = ''
    elif config.sync:
        param = 'cluster_sync'
        cmd.append(config.sync_arg)
    else:
        param = 'cluster'

    if cmd[0] and not shutil.which(cmd[0]):
        raise click.UsageError(f"""
        The configured cluster submission command '{cmd[0]}' is does not
        exist or is not executable. Please check your cluster configuration.
        """)

    config[param] = cfg.expand(" ".join(cmd))

    rval = start_snakemake(config)
    if not rval:
        sys.exit(1)
