#!/usr/bin/env python3

import click
from pkg_resources import resource_filename
import snakemake
import os
import shutil
import sys
import logging
from coloredlogs import ColoredFormatter
import functools

import ymp
from ymp.common import update_dict
from ymp.util import AttrDict


# Set up Logging

log = logging.getLogger("ymp")
log.setLevel(logging.WARNING)
log_handler = logging.StreamHandler()
log_handler.setLevel(logging.DEBUG)  # no filtering here
formatter = ColoredFormatter("YMP: %(message)s")
log_handler.setFormatter(formatter)
log.addHandler(log_handler)

# We could get snakemake's logging output like so:
# slog = logging.getLogger("snakemake")
# slog.parent = log
#
# Or use the log_handler parameter to snakemake to override
# turning log events into messages. That would take some rewriting
# of snakemake code though.
#
# There seems to be no good way of redirecting snakemakes logging
# in a python-logging style way as it installs its own stream
# handlers once control has passed to snakemake.


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


def nohup():
    """
    Make YMP continue after the shell dies.

    - redirects stdout and stderr into pipes and sub process that won't
      die if it can't write to either anymore
    - closes stdin

    """
    import signal
    from select import select
    from multiprocessing import Process

    # ignore sighup
    signal.signal(signal.SIGHUP, signal.SIG_IGN)

    # sighup is actually only sent by bash if bash gets it itself, so not if
    # `exit` is called with ymp in the background. What usually kills that type
    # of process is trying to write to stdout or stderr. We need to catch that.

    # close stdin (don't need that anyway)
    sys.stdin.close()

    # redirect stdout and err into a pipe and save original target
    pipes = {}
    for fd in (sys.stdout, sys.stderr):
        # save original std fd
        saved = os.dup(fd.fileno())
        # create a pipe
        pipe = os.pipe()
        # overwrite std fd with one end of pipeo
        os.dup2(pipe[1], fd.fileno())
        # save other end and target std fd
        pipes[pipe[0]] = saved

    def watcher():
        while True:
            r, w, x = select(pipes.keys(), [], [], 60)
            for pipe in pipes:
                if pipe in r:
                    data = os.read(pipe, 4096)
                    try:
                        os.write(pipes[pipe], data)
                    except IOError:
                        pass

    p = Process(target=watcher)
    p.start()


def snake_params(func):
    """Default parameters for subcommands launching Snakemake"""
    @click.argument("targets", nargs=-1, metavar="TARGET_FILES")
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
    @click.option(
        "--nohup", "-N", is_flag=True,
        help="Don't die once the terminal goes away.",
        callback=lambda ctx, param, val: nohup() if val else None
    )
    @click.option(
        "--verbose", "-v", count=True,
        help="Increase verbosity. May be specified multiple times.",
        callback=lambda ctx, param, val: log.setLevel(
            max(log.getEffectiveLevel() - 10 * val,
                logging.DEBUG)
        )
    )
    @click.option(
        "--quiet", "-q", count=True,
        help="Decrease verbosity. May be specified multiple times.",
        callback=lambda ctx, param, val: log.setLevel(
            min(log.getEffectiveLevel() + 10 * val,
                logging.CRITICAL)
        )
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

    for arg in ('use_drmaa', 'qsub_sync', 'qsub_sync_arg',
                'qsub_cmd', 'qsub_args', 'nohup'):
        if arg in kwargs:
            del kwargs[arg]

    if log.getEffectiveLevel() > logging.WARNING:
        kwargs['quiet'] = True
    if log.getEffectiveLevel() < logging.WARNING:
        kwargs['verbose'] = True
    kwargs['use_conda'] = True
    if 'targets' in kwargs:
        kwargs['targets'] = [os.path.join(prefix, t)
                             for t in kwargs['targets']]
    return snakemake.snakemake(
        resource_filename("ymp", "rules/Snakefile"),
        **kwargs)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version=ymp.__release__)
def cli():
    """
    Welcome to YMP!

    Please find the full manual at https://ymp.readthedocs.io
    """


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
    from ymp.config import icfg
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
    import ymp.env  # imported for subcommands
    ymp.env  # silence flake8 warning re above line


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


@env.command(context_settings=CONTEXT_SETTINGS)
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


@env.command(context_settings=CONTEXT_SETTINGS)
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


@env.command(context_settings=CONTEXT_SETTINGS)
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


@env.command(context_settings=CONTEXT_SETTINGS)
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

    if envname not in ymp.env.by_name:
        log.critical("Environment '%s' unknown", envname)
        sys.exit(1)

    env = ymp.env.by_name[envname]
    if not os.path.exists(env.path):
        log.warning("Environment not yet installed")
        env.create()

    sys.exit(ymp.env.by_name[envname].run(command))
