"""
ymp.sphinxext
~~~~~~~~~~~~~

Sphinx extension automatically inserting docstrings from snakemake
rules into the Sphinx doctree.
"""

from docutils import nodes, statemachine
from docutils.parsers import rst

from ymp.snakemake import ExpandableWorkflow

from sphinx.util import logging


try:
    logger = logging.getLogger(__name__)
except AttributeError:
    # Fall back to normal logging
    import logging as _logging
    logger = _logging.getLogger(__name__)


class SnakefileDirective(rst.Directive):
    """
    rST Directive ``.. snakefile [filename]``

    Extracts docstrings from rules in snakefile and auto-generates
    documentation.
    """

    has_content = False
    required_arguments = 1

    def run(self):
        """Entry point"""
        logger.error("running SnakefileDirective")
        snakefile = self.arguments[0]

        rules = self._load_snakefile(snakefile)

        return self._generate_nodes(snakefile, rules)

    def _load_snakefile(self, file_path):
        """Load the Snakefile"""
        logger.error("loading snakefile {}".format(file_path))
        workflow = ExpandableWorkflow(snakefile=file_path)
        workflow.include(file_path)
        return workflow.rules

    def _generate_nodes(self, snakefile, rules):
        """Generate Sphinx nodes from parsed snakefile"""

        section = nodes.section(
            '',
            nodes.title("thetitle"),
            ids=[nodes.make_id(snakefile)],
            names=[nodes.fully_normalize_name(snakefile)]
        )

        result = statemachine.ViewList()

        for rule in rules:
            result.append(".. class:: {}".format(rule.name), snakefile)
            result.append("", snakefile)
            if rule.docstring:
                for line in rule.docstring.splitlines():
                    result.append("   " + line, snakefile)
                result.append("", snakefile)

        self.state.nested_parse(result, 0, section)

        return [section]


def setup(app):
    app.add_directive('snakefile', SnakefileDirective)
