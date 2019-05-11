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
from ymp.exceptions import YmpException, YmpStageError
from ymp.stage import StageStack

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


DEBUG_LOGFILE_NAME = os.environ.get("YMP_DEBUG_EXPAND")
if DEBUG_LOGFILE_NAME:
    import time
    start_time = time.time()
    if DEBUG_LOGFILE_NAME == "stderr":
        DEBUG_LOGFILE = sys.stderr
    else:
        DEBUG_LOGFILE = open(DEBUG_LOGFILE_NAME, "a")


def debug(msg, *args, **kwargs):
    if DEBUG_LOGFILE_NAME:
        tim = (time.time() - start_time)
        msg = "{:4.4f}: " + msg
        DEBUG_LOGFILE.write(msg.format(tim, *args, **kwargs) + '\n')


debug("started")


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
        result: list = []

        stack, _, tocomplete = incomplete.rpartition(".")
        debug("complete(stack={},incomplete={})", stack, tocomplete)

        if not stack:
            cfg = ymp.get_config()
            options = cfg.projects.keys()
            result += (o for o in options if o.startswith(tocomplete))
            result += (o + "." for o in options if o.startswith(tocomplete))
        else:
            from ymp.stage import StageStack
            try:
                stackobj = StageStack.get(stack)
            except YmpStageError as e:
                debug(e.format_message())
                return []
            debug("stacko = {}", repr(stack))
            options = stackobj.complete(tocomplete)
            debug("options = {}", options)
            # reduce items sharing prefix before "_"
            prefixes = {}
            for option in options:
                prefix = option.split("_", 1)[0]
                group = prefixes.setdefault(prefix, [])
                group.append(option)
            if len(prefixes) == 1:
                extensions = options
            else:
                extensions = []
                for prefix, group in prefixes.items():
                    if len(group) > 1:
                        extensions.append(prefix + "_")
                    else:
                        extensions.append(group[0])
            result += ('.'.join((stack, ext)) for ext in extensions)
            result += ('.'.join((stack, ext))+"." for ext in extensions
                       if not ext[-1] == "_")

        debug("res={}", result)
        return result


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
    if not cfg.projects:
        log.warning("No projects configured. Are you in the right folder?")
        log.warning("  Config files loaded:")
        for fname in cfg.conffiles:
            log.warning("    - %s", fname)

    root_path = cfg.root
    cur_path = os.path.abspath(os.getcwd())
    if not cur_path.startswith(root_path):
        raise YmpException("internal error - CWD moved out of YMP root?!")
    cur_path = cur_path[len(root_path):]

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
    kwargs['workdir'] = root_path

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
        if cur_path:
            kwargs['targets'] = [os.path.join(cur_path, t)
                                 for t in kwargs['targets']]
        else:
            targets = []
            for t in kwargs['targets']:
                try:
                    stack = StageStack.get(t)
                    targets.append(os.path.join(t, 'all_targets.stamp'))
                except YmpStageError as e:
                    #log.exception("asd")
                    targets.append(t)
            kwargs['targets'] = targets

    log.debug("Running snakemake.snakemake with args: %s", kwargs)

    # A snakemake workflow was created above to resolve the
    # stage stack. Unload it so things run correctly from within
    # snakemake.
    cfg.unload()

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

    config.add_layer("<computed>", {param: cfg.expand(" ".join(cmd))})

    rval = start_snakemake(config)
    if not rval:
        sys.exit(1)
