"Implements subcommands for ``ymp show``"

import logging

import click

import ymp
from ymp.cli.shared_options import command

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class ConfigPropertyParam(click.ParamType):
    """Handles tab expansion for ``ymp show`` arguments"""
    _properties = None

    @property
    def properties(self):
        """Find properties offered by ConfigMgr"""
        if not self._properties:
            from ymp.config import ConfigMgr
            self._properties = {
                prop: getattr(getattr(ConfigMgr, prop), "__doc__")
                for prop in dir(ConfigMgr)
                if (prop[0] != "_"
                    and isinstance(getattr(ConfigMgr, prop), property))
            }
        return self._properties

    def complete(self, _ctx, incomplete):
        """Try to complete incomplete command

        This is executed on tab or tab-tab from the shell

        Args:
          ctx: click context object
          incomplete: last word in command line up until cursor

        Returns:
          list of words incomplete can be completed to
        """
        # This only list public properties, no member variables,
        # mainly because member variables can't have docstrings set.

        return [x for x in self.properties.keys()
                if x.startswith(incomplete)]

    def convert(self, value, param, ctx):
        """Convert value of param given context

        Args:
          value: string passed on command line
          param: click parameter object
          ctx:   click context object
        """
        return value

    def __repr__(self):
        props = "\n".join(
            "  {}: {}".format(p, self.properties[p].strip())
            for p in self.properties
            if self.properties[p]
        )
        return "\n".join(["Properties:", props])


def show_help(ctx, _param=None, value=True):
    """Display click command help"""
    if value:
        helpstr = [ctx.get_help(), '']
        arg_docs = [repr(param.type)
                    for param in ctx.command.params
                    if isinstance(param, click.Argument)]
        click.echo("\n".join(helpstr + arg_docs), color=ctx.color)
        ctx.exit()


@command(add_help_option=False)
@click.argument(
    "prop", nargs=1, metavar="PROPERTY", required=False,
    type=ConfigPropertyParam()
)
@click.option(
    "--help", "-h", callback=show_help, expose_value=False, is_flag=True
)
@click.option(
    "--source", "-s", is_flag=True,
    help="Show source"
)
@click.pass_context
def show(ctx, prop, source):
    """
    Show configuration properties
    """
    if not prop:
        show_help(ctx)

    log.debug(f"querying prop {prop}")
    obj = ymp.get_config()
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

    try:
        output = obj.to_yaml(source)
    except AttributeError:
        output = str(obj)

    click.echo(output)
