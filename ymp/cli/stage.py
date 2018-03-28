import logging
from fnmatch import fnmatch

import click

from ymp.cli.shared_options import group

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


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
@click.argument(
    "stage_opt", metavar="STAGE", required=False,
)
def ls(long_opt, short_opt, stage_opt, code_opt):
    """
    List available stages
    """
    if long_opt and short_opt:
        raise click.UsageError(
            "Options --long and --short are mutually exclusive")

    from ymp.stage import Stage
    all_stages = Stage.get_stages()
    if stage_opt:
        stages = [all_stages[m] for m in all_stages if fnmatch(m, stage_opt)]
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
                long_doc = doc[1]
            else:
                long_doc = ""
        else:
            short_doc = "[no docs]"
            long_doc = ""

        if long_doc and long_opt:
            description = "\n{}\n\n".format(long_doc)
        else:
            description = ""

        if short_doc and not short_opt:
            summary = "  " + short_doc
        else:
            summary = ""

        if code_opt:
            code = "\nfrom {}:{}".format(stage.filename, stage.lineno)
        else:
            code = ""

        print("{name:<{width}}{summary}{code}{description}"
              "".format(name=stage.name,
                        width=name_width,
                        summary=summary,
                        code=code,
                        description=description))
