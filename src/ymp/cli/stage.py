import logging
import textwrap
from fnmatch import fnmatch

import click

from ymp.cli.shared_options import group

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


def wrap(header, data):
    wrapper = textwrap.TextWrapper(
        initial_indent=header + " ",
        subsequent_indent=" " * len(header) + " "
    )
    return "\n"+"\n".join(wrapper.wrap(" ".join(data)))


@group()
def stage():
    """
    Manipulate YMP stages
    """


@stage.command(name="list")
@click.option(
    "--long", "-l", "long_opt", is_flag=True,
    help="Show full stage descriptions"
)
@click.option(
    "--short", "-s", "short_opt", is_flag=True,
    help="Show only stage names"
)
@click.option(
    "--code", "-c", "code_opt", is_flag=True,
    help="Show definition file name and line number"
)
@click.option(
    "--types", "-t", "type_opt", is_flag=True,
    help="Show input/output types"
)
@click.argument(
    "stage_opt", metavar="STAGE", nargs=-1
)
def ls(long_opt, short_opt, stage_opt, code_opt, type_opt):
    """
    List available stages
    """
    if long_opt and short_opt:
        raise click.UsageError(
            "Options --long and --short are mutually exclusive")

    from ymp.stage import Stage
    all_stages = Stage.get_registry()
    if stage_opt:
        stages = [all_stages[m] for m in all_stages
                  if any(fnmatch(m, pat) for pat in stage_opt)]
    else:
        stages = list(all_stages.values())
    stages = sorted(list(set(stages)), key=lambda s: s.name)

    if not stages:  # nothing to show
        return

    name_width = max(len(x.name) for x in stages)
    for stage in stages:
        if hasattr(stage, 'docstring'):
            doc = stage.docstring.strip().split("\n", 1)
            short_doc = doc[0].strip()
            if len(doc) > 1:
                long_doc = textwrap.dedent(doc[1])
            else:
                long_doc = ""
        else:
            short_doc = "[no docs]"
            long_doc = ""

        if long_doc and long_opt:
            description = "\n".join("  " + l for l in long_doc.split("\n"))
        else:
            description = ""

        if short_doc and not short_opt:
            summary = "  " + short_doc
        else:
            summary = ""

        if code_opt:
            code = "\n  defined in: {}:{}".format(stage.filename, stage.lineno)
        else:
            code = ""

        if type_opt:
            dtypes = (wrap("  inputs: ", stage.inputs) +
                      wrap("  outputs:", stage.outputs))
        else:
            dtypes = ""

        print("{name:<{width}}{summary}{description}{code}{dtypes}\n"
              "".format(name=stage.name,
                        width=name_width,
                        summary=summary,
                        code=code,
                        dtypes=dtypes,
                        description=description))
