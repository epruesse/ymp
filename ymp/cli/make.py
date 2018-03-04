import functools
import logging
import os
import sys

import click

from pkg_resources import resource_filename

import snakemake

from ymp.cli.shared_options import command, nohup_option
from ymp.common import update_dict, Cache, AttrDict
from ymp.exceptions import YmpException

log = logging.getLogger(__name__)


class TargetParam(click.ParamType):
    def complete(_, ctx, incomplete):
        # log = open("err.txt", "a")
        log = open("/dev/null", "a")
        log.write("\nincomplete={}\n".format(incomplete))
        cache = Cache.get_cache("completion")
        query_stages = incomplete.split(".")
        log.write("stages={}\n".format(query_stages))
        options = []

        if len(query_stages) == 1:  # expand projects
            from ymp.config import icfg
            options = icfg.datasets
        else:  # expand stages
            if 'stages' in cache:
                stages = cache['stages']
            else:
                from ymp.snakemake import load_workflow
                from ymp.stage import Stage
                load_workflow()
                stages = cache['stages'] = list(Stage.get_stages().keys())
            options = stages
        options = [o for o in options if o.startswith(query_stages[-1])]
        prefix = ".".join(query_stages[:-1])
        if prefix:
            prefix += "."
        log.write("prefix={}\n".format(prefix))
        options = [prefix + o + cont for o in options for cont in ("/", ".")]
        log.write("options={}\n".format(options))
        log.close()
        return options


def snake_params(func):
    """Default parameters for subcommands launching Snakemake"""
    @click.argument(
        "targets", nargs=-1, metavar="TARGET_FILES",
        type=TargetParam()
    )
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
    @nohup_option
    @functools.wraps(func)
    def decorated(*args, **kwargs):
        return func(*args, **kwargs)
    return decorated


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
    log.debug("Running snakemake.snakemake with args: {}".format(kwargs))
    return snakemake.snakemake(
        resource_filename("ymp", "rules/Snakefile"),
        **kwargs)


@command()
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


@command()
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
