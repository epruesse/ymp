"Implements subcommands for ``ymp init``"

import logging
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
