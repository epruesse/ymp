import logging

import click

from ymp.cli.shared_options import command
from ymp.config import ConfigMgr, icfg

log = logging.getLogger(__name__)


# This only list public properties, no member variables,
# mainly because member variables can't have docstrings set.
properties = {
    prop: getattr(getattr(ConfigMgr, prop), "__doc__")
    for prop in dir(ConfigMgr)
    if (prop[0] != "_"  # no private attrs
        and isinstance(getattr(ConfigMgr, prop), property))  # only properties
}


class ConfigPropertyParam(click.ParamType):
    def complete(_, ctx, incomplete):
        """Try to complete incomplete command

        This is executed on tab or tab-tab from the shell

        Args:
          ctx: click context object
          incomplete: last word in command line up until cursor

        Returns:
          list of words incomplete can be completed to
        """
        return [x for x in properties.keys()
                if x.startswith(incomplete)]

    def convert(_, value, param, ctx):
        """Convert value of param given context

        Args:
          value: string passed on command line
          param: click parameter object
          ctx:   click context object
        """
        return value

    def __repr__(self):
        props = "\n".join(
            "  {}: {}".format(p, properties[p].strip())
            for p in properties
            if properties[p]
        )
        return "\n".join(["Properties:", props])


def show_help(ctx, param=None, value=True):
    if value:
        help = [ctx.get_help(), '']
        arg_docs = [repr(param.type)
                    for param in ctx.command.params
                    if isinstance(param, click.Argument)]
        click.echo("\n".join(help + arg_docs), color=ctx.color)
        ctx.exit()


@command(add_help_option=False)
@click.argument(
    "prop", nargs=1, metavar="PROPERTY", required=False,
    type=ConfigPropertyParam()
)
@click.option(
    "--help", "-h", callback=show_help, expose_value=False, is_flag=True
)
@click.pass_context
def show(ctx, prop):
    """
    Show configuration properties
    """
    if not prop:
        show_help(ctx)

    log.error(f"querying prop {prop}")
    obj = icfg
    while prop:
        key, _, prop = prop.partition(".")
        key, _, slice_str = key.partition("[")
        obj = getattr(obj, key)

        if not slice_str:
            continue

        for subslice_str in slice_str[:-1].replace("][", ",").split(","):
            try:
                indices = [int(part) if part else None
                           for part in subslice_str.split(":")]
                if len(indices) > 3:
                    log.warning(f"Malformed slice string '{slice_str}'")
                    indices = indices[:3]
                if len(indices) == 1:
                    obj = obj[indices[0]]
                else:
                    obj = obj[slice(*indices)]
            except ValueError:
                obj = obj[subslice_str]

    click.echo("---")
    click.echo(type(obj))
    click.echo(obj)
