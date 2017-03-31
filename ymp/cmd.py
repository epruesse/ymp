#!/usr/bin/env python3

import click
from pkg_resources import resource_filename
import snakemake
import os

"""
    cores=1,
    dryrun=False,
    keepgoing=False,
    verbose=False,
    use_conda=False,

snakemake_args = {
    listrules=False,
    list_target_rules=False,
    nodes=1,
    local_cores=1,
    resources={},
    config={},
    configfile=None,
    config_args=None,
workdir=None,
    targets=None,
    touch=False,
    forcetargets=False,
    forceall=False,
    forcerun=[],
    until=[],
    omit_from=[],
    prioritytargets=[],
    stats=None,
    printreason=False,
    printshellcmds=False,
    printdag=False,

    printrulegraph=False,
    printd3dag=False,
    nocolor=False,
    quiet=False,
    cluster=None,
    cluster_config=None,
    cluster_sync=None,
    drmaa=None,
    jobname='snakejob.{rulename}.{jobid}.sh',
    immediate_submit=False,
    standalone=False,
    ignore_ambiguity=False,
    snakemakepath=None,
    lock=True,
    unlock=False,
    cleanup_metadata=None,
    force_incomplete=False,
    ignore_incomplete=False,
    list_version_changes=False,
    list_code_changes=False,
    list_input_changes=False,
    list_params_changes=False,
    list_resources=False,
    summary=False,
    archive=None,
    detailed_summary=False,
    latency_wait=3,
    benchmark_repeats=1,
    wait_for_files=None,
    print_compilation=False,
    debug=False,
    notemp=False,
    keep_remote_local=False,
    nodeps=False,
    keep_target_files=False,
    keep_shadow=False,
    allowed_rules=None,
    jobscript=None,
    timestamp=False,
    greediness=None,
    no_hooks=False,
    overwrite_shellcmd=None,
    updated_files=None,
    log_handler=None,
    keep_logger=False,
    max_jobs_per_second=None,
    restart_times=0,
    force_use_threads=False,
    mode=0,
    wrapper_prefix=None
}
"""

@click.group()
def cli():
    pass

@cli.command()
@click.argument("targets", nargs=-1, metavar="FILES")
@click.option("--dryrun", "-n", default=False, is_flag=True)
@click.option("--printshellcmds", "-p", default=False, is_flag=True)
@click.option("--cores", "-j", "nodes", default=1)
@click.option("--keepgoing", "-k", default=False, is_flag=True)
@click.option("--verbose", "-v", default=False, is_flag=True)
@click.option("--use-conda/--skip-conda", default=True)
@click.option("--lock/--no-lock")
@click.option("--cluster-config", "-u", default="cluster.yaml")
@click.option("--rerun-incomplete","--ri", 'force_incomplete', is_flag=True)
@click.option("--latency-wait","-w", default=0)
def make(**kwargs):
    "generate target files"
    snakemake.snakemake(resource_filename("ymp", "rules/Snakefile"), **kwargs)

@cli.command()
@click.argument("targets", nargs=-1, metavar="FILES")
@click.option("--dryrun", "-n", default=False, is_flag=True)
@click.option("--printshellcmds", "-p", default=False, is_flag=True)
@click.option("--cores", "-j", "nodes", default=1024)
@click.option("--keepgoing", "-k", default=False, is_flag=True)
@click.option("--verbose", "-v", default=False, is_flag=True)
@click.option("--use-conda/--skip-conda", default=True)
@click.option("--lock/--no-lock")
@click.option("--cluster-config", "-u", default="cluster.yaml")
@click.option("--rerun-incomplete","--ri", 'force_incomplete', is_flag=True)
@click.option("--latency-wait","-w", default=0)
@click.option("--local-cores", default=8)
def submit(**kwargs):
    "generate target files"
    drmaa = " ".join([
        '-l nodes=1:ppn={threads}',
        '-j oe',
        '-M elmar.pruesse@ucdenver.edu',
        '-l walltime={cluster.walltime}',
        '-l mem={cluster.mem}',
        '-q {cluster.queue}'
    ])
    print(repr(kwargs))
    snakemake.snakemake(resource_filename("ymp", "rules/Snakefile"), drmaa=drmaa, **kwargs)
