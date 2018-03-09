import click
import click_completion

import ymp
from ymp.cli.env import env
from ymp.cli.make import make, submit
from ymp.cli.shared_options import group
from ymp.cli.stage import stage
from ymp.cli.show import show

click_completion.init()


def install_completion(ctx, attr, value):
    """Installs click_completion tab expansion into users shell"""
    if not value or ctx.resilient_parsing:
        return value
    shell, path = click_completion.install()
    click.echo('%s completion installed in %s' % (shell, path))
    exit(0)


@group()
@click.version_option(version=ymp.__version__)
@click.option(
    '--install-completion', is_flag=True,
    callback=install_completion, expose_value=False,
    help="Install command completion for the current shell. "
         "Make sure to have psutil installed.")
def main(**kwargs):
    """
    Welcome to YMP!

    Please find the full manual at https://ymp.readthedocs.io
    """


main.add_command(make)
main.add_command(submit)
main.add_command(env)
main.add_command(stage)
main.add_command(show)
