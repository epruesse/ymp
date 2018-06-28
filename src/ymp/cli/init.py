"Implements subcommands for ``ymp init``"

import logging
import os
import shutil
import subprocess as sp

import click

import ymp
from ymp.cli.shared_options import command, group

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


@group()
def init():
    """Initialize YMP workspace"""


def have_command(cmd):
    try:
        sp.run("command -v " + cmd, shell=True, stdout=sp.DEVNULL, check=True)
    except sp.CalledProcessError:
        log.debug("Command '%s' not found", cmd)
        return False
    log.debug("Command '%s' found", cmd)
    return True


@init.command()
@click.option("--yes", "-y", is_flag=True, help="Confirm every prompt")
def cluster(yes):
    """Set up cluster"""
    cfg = ymp.get_config()._config

    if cfg.cluster.profile is not None and not yes:
        click.confirm("Cluster profile '{}' already configured. "
                      "Do you want to overwrite this setting?"
                      "".format(cfg.cluster.profile),
                      abort=True)

    log.warning("Trying to detect cluster type")
    log.debug("Checking for SLURM")
    if have_command("sbatch"):
        log.warning("Found SLURM. Updating config.")
        cfg["cluster"]["profile"] = "slurm"
    elif have_command("qsub"):
        log.warning("Detected SGE or Torque")
    else:
        log.warning("No cluster submit commands found")
        cfg["cluster"]["profile"] = None
    log.warning("Saving config")
    cfg.save()


@init.command()
@click.argument("name", required=False)
@click.option("--yes", "-y", is_flag=True, help="Confirm every prompt")
def project(name, yes):
    cfg = ymp.get_config()._config

    if not name:
        name = click.prompt("Please enter a name for the new project",
                            type=str)

    if name in cfg.projects and not yes:
        click.confirm("Project '{}' already configured. "
                      "Do you want to overwrite this project?"
                      "".format(name),
                      abort=True)

    cfg.projects[name] = {'data': None}
    log.warning("Saving config")
    cfg.save()


@init.command()
def demo():
    """
    Copies YMP tutorial data into the current working directory
    """
    click.echo("Copying tutorial data to current working directory...")
    cwd_path = os.getcwd()
    cwd_files = os.listdir(cwd_path)
    demo_path = os.path.join(ymp._rsc_dir, "demo")
    demo_files = os.listdir(demo_path)
    conflicts = [f for f in demo_files if f in cwd_files]
    if len(cwd_files) > 10:
        click.confirm(
            "WARNING: "
            "The current working directory already contains a lot of files.\n"
            "  Using an empty directory to start with is highly suggested.\n"
            "  Do you want to continue?", abort=True)
    if conflicts:
        click.confirm(
            "WARNING: "
            "This operation would overwrite the following files: {}\n"
            "  Do you want to continue?".format(conflicts), abort=True)
        for f in conflicts:
            if os.path.isdir(f):
                shutil.rmtree(f)
            else:
                os.unlink(f)
    for f in demo_files:
        src = os.path.join(demo_path, f)
        dst = os.path.join(cwd_path, f)
        if os.path.isdir(src):
            shutil.copytree(src, dst)
        else:
            shutil.copy(src, dst)
    click.echo("done.")
    click.echo("")
    click.echo("Try running 'ymp make toy.assemble_megahit.map_bbmap',")
    click.echo("or see https://ymp.readthedocs.io for more examples.")
