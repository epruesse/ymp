import click

from setuptools_scm import get_version

import ymp
from ymp.cli.env import env
from ymp.cli.make import make, submit
from ymp.cli.shared_options import group
from ymp.cli.stage import stage


@group()
@click.version_option(version=ymp.__version__)
def main(**kwargs):
    """
    Welcome to YMP!

    Please find the full manual at https://ymp.readthedocs.io
    """


main.add_command(make)
main.add_command(submit)
main.add_command(env)
main.add_command(stage)
