import logging

from pkg_resources import resource_filename

from ymp.cli.shared_options import group
from ymp.stage import Stage
from ymp.snakemake import ExpandableWorkflow

log = logging.getLogger(__name__)


@group()
def stage():
    """
    """

@stage.command()
def list():
    snakefile = resource_filename("ymp", "rules/Snakefile")
    workflow = ExpandableWorkflow(snakefile=snakefile)
    workflow.include(snakefile)
    for stage in Stage.get_stages().values():
        print(stage)
        if hasattr(stage, 'doc'):
            print(stage.doc)
    

