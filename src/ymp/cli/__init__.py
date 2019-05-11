import click
import click_completion

import ymp
from ymp.cli.env import env
from ymp.cli.make import make, submit
from ymp.cli.shared_options import group
from ymp.cli.stage import stage
from ymp.cli.show import show
from ymp.cli.init import init

click_completion.init()


def install_completion(ctx, attr, value):
    """Installs click_completion tab expansion into users shell"""
    if not value or ctx.resilient_parsing:
        return value
    shell, path = click_completion.install()
    click.echo('%s completion installed in %s' % (shell, path))
    exit(0)


def install_profiler(ctx, attr, value):
    if not value or ctx.resilient_parsing:
        return value
    import yappi
    import atexit

    def dump_profile():
        profile = yappi.get_func_stats()
        profile.sort("ttot")
        profile.print_all(out=value, columns={
            0: ("name", 120),
            1: ("ncall", 10),
            2: ("tsub", 8),
            3: ("ttot", 8),
            4: ("tavg", 8)})

    yappi.start()
    atexit.register(dump_profile)


@group()
@click.version_option(version=ymp.__version__)
@click.option(
    '--install-completion', is_flag=True,
    callback=install_completion, expose_value=False,
    help="Install command completion for the current shell. "
         "Make sure to have psutil installed.")
@click.option(
    '--profile', type=click.File(mode="w"),
    callback=install_profiler, expose_value=False,
    help="Profile execution time using Yappi"
)
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
main.add_command(init)
