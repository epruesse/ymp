"""
ymp.sphinxext
~~~~~~~~~~~~~

Sphinx extension automatically inserting docstrings from snakemake
rules into the Sphinx doctree.
"""

import os

from docutils import nodes, statemachine
from docutils.parsers import rst

from sphinx import addnodes
from sphinx.directives import ObjectDescription
from sphinx.domains import Domain, ObjType
from sphinx.roles import XRefRole
from sphinx.util import logging, ws_re
from sphinx.util.nodes import make_refnode

from ymp.snakemake import ExpandableWorkflow
from ymp.snakemakelexer import SnakemakeLexer


try:
    logger = logging.getLogger(__name__)
except AttributeError:
    # Fall back to normal logging
    import logging as _logging
    logger = _logging.getLogger(__name__)


class SnakemakeRule(ObjectDescription):
    option_spec = {
        'source': rst.directives.unchanged
    }

    def handle_signature(self, sig, signode):
        """
        Parse rule signature *sig* into RST nodes and append them
        to *signode*.

        The retun value identifies the object and is passed to
        :meth:`add_target_and_index()` unchanged
        """
        rawsource = sig
        text = "rule {}".format(sig)
        signode += addnodes.desc_name(rawsource, text)

        if 'source' in self.options:
            self.add_source_link(signode)

        sigid = ws_re.sub('', sig)
        return sigid

    def add_source_link(self, signode):
        filename, lineno = self.options['source'].split(':')
        if not hasattr(self.env, '_snakefiles'):
            self.env._snakefiles = set()
        self.env._snakefiles.add(filename)

        onlynode = addnodes.only(expr='html')  # show only in html
        onlynode += nodes.reference(
            '',
            refuri='_snakefiles/{}.html#line-{}'.format(filename, lineno)
        )
        onlynode[0] += nodes.inline('', '[source]',
                                    classes=['viewcode-link'])
        signode += onlynode

    def add_target_and_index(self, name, sig, signode):
        """
        Add cross-reference IDs and entries to self.indexnode
        """
        targetname = "-".join((self.objtype, name))
        if targetname not in self.state.document.ids:
            signode['names'].append(targetname)
            signode['ids'].append(targetname)
            signode['first'] = (not self.names)
            self.state.document.note_explicit_target(signode)

            objects = self.env.domaindata[self.domain]['objects']
            key = (self.objtype, name)
            if key in objects:
                self.env.warn(self.env.docname,
                              'duplicate description of {} {}, '
                              'other instance in {}:{}'
                              ''.format(self.objtype, name,
                                        self.env.doc2path(objects[key]),
                                        self.lineno))
            objects[key] = self.env.docname

        # register rule in index
        indextext = self.get_index_text(self.objtype, name)
        if indextext:
            self.indexnode['entries'].append((
                'single',
                indextext,
                targetname,
                '',
                None))

    def get_index_text(self, objectname, name):
        return "{} ({})".format(name, objectname)


class SnakemakeDomain(Domain):
    """Snakemake language domain."""
    name = "sm"
    label = "Snakemake"

    object_types = {
        # ObjType(name, *roles, **attrs)
        'rule': ObjType('rule', 'rule'),
    }
    directives = {
        'rule': SnakemakeRule,
    }
    roles = {
        'rule': XRefRole(),
    }
    initial_data = {
        'objects': {},  # (type, name) -> docname, labelid
    }

    data_version = 0

    def clear_doc(self, docname):
        if 'objects' in self.data:
            for key, dn in self.data['objects'].items():
                if dn == docname:
                    del self.data['objects'][key]

    def resolve_xref(self, env, fromdocname, builder,
                     typ, target, node, contnode):
        objects = self.data['objects']
        objtypes = self.objtypes_for_role(typ)
        for objtype in objtypes:
            if (objtype, target) in objects:
                return make_refnode(builder, fromdocname,
                                    objects[objtype, target],
                                    objtype + "-" + target,
                                    contnode, target + ' ' + objtype)

    def get_objects(self):
        for (typ, name), docname in self.data['objects'].items():
            # name, dispname, type, docname, anchor, searchprio
            yield name, name, typ, docname, typ + '-' + name, 1


class AutoSnakefileDirective(rst.Directive):
    """
    rST Directive ``.. autosnake [filename]``

    Extracts docstrings from rules in snakefile and auto-generates
    documentation.
    """

    has_content = False
    required_arguments = 1

    def run(self):
        """Entry point"""
        snakefile = self.arguments[0]

        rules = self._load_snakefile(snakefile)

        return self._generate_nodes(snakefile, rules)

    def _load_snakefile(self, file_path):
        """Load the Snakefile"""
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
            fn = os.path.relpath(rule.snakefile, "..")
            line = rule.lineno
            name = rule.name
            result.append(".. sm:rule:: {}".format(name), snakefile)
            result.append("   :source: {}:{}".format(fn, line), snakefile)
            result.append("", snakefile)
            if rule.docstring:
                for line in rule.docstring.splitlines():
                    result.append("   " + line, snakefile)
                result.append("", snakefile)
        result.append("", snakefile)

        self.state.nested_parse(result, 0, section)

        return [section]


def collect_pages(app):
    if not hasattr(app.env, '_snakefiles'):
        return

    highlight_block = app.builder.highlighter.highlight_block

    for snakefile in app.env._snakefiles:
        try:
            with open(os.path.join("..", snakefile), 'r') as f:
                code = f.read()
        except IOError:
            logger.error("failed to open {}".format(snakefile))
            continue
        highlighted = highlight_block(code, 'snakemake', lineanchors="line")
        context = {
            'title': snakefile,
            'body': '<h1>Snakefile "{}"</h1>'.format(snakefile) +
            highlighted
        }
        yield (os.path.join('_snakefiles', snakefile), context, 'page.html')

    html = ['\n']
    context = {
        'title': ('Overview: Snakemake rule files'),
        'body': '<h1>All Snakemake rule files</h1>' +
        ''.join(html)
    }
    yield ('_snakefiles/index', context, 'page.html')


def setup(app):
    app.add_lexer('snakemake', SnakemakeLexer())
    app.add_domain(SnakemakeDomain)
    app.add_directive('autosnake', AutoSnakefileDirective)
    app.connect('html-collect-pages', collect_pages)
