#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# YMP documentation build configuration file

# This file is execfile()d with the current directory set to its
# containing dir.

import os
import sys
sys.path.insert(0, os.path.join(os.path.abspath(os.pardir), 'src'))

import ymp
import cloud_sptheme as csp

docdir = os.path.dirname(__file__)
ympdir = os.path.join(os.path.dirname(docdir), 'src')



# -- General configuration ------------------------------------------------

needs_sphinx = '1.6'


extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx_autodoc_typehints',
    'sphinx.ext.intersphinx',
    'sphinx.ext.coverage',
    'sphinx.ext.viewcode',
    'sphinxcontrib.fulltoc',
    'sphinx_click.ext',
    'ymp.sphinxext',
]

autoclass_content = "both"
default_role = "any"

templates_path = ['_templates']
html_static_path = ['_static']
source_suffix = ['.rst']
master_doc = 'contents'  # master toc
index_doc = 'index'  # frontpage

project = 'YMP Extensible Omics Pipeline'
author = 'Elmar Pruesse'
copyright = '2017-2018, ' + author

version = ".".join(ymp.__version__.split(".")[:3])
release = ymp.__version__

language = None

exclude_patterns = [
    '_build',
    'Thumbs.db',
    '.DS_Store'
]
exclude_patterns.extend(templates_path)
exclude_patterns.extend(html_static_path)

pygments_style = 'sphinx'
todo_include_todos = True
keep_warnings = True
issue_tracker_url = 'gh:epruesse/ymp'

modindex_common_prefix = [ "ymp." ]

html_theme = "cloud"
html_theme_path = [csp.get_theme_dir()]
html_theme_options = {
    'roottarget': index_doc,
    'borderless_decor': True,
    'sidebarwidth': "3in",
    'hyphenation_language': 'en',
    #'rubricbgcolor': '#b3b3b3',
}
# html_title
# html_short_title
# html_logo
# html_favicon


# Custom sidebar templates, must be a dictionary that maps document names
# to template names.
html_sidebars = {'**': ['searchbox.html', 'globaltoc.html']}

# -- Options for HTMLHelp output ------------------------------------------

htmlhelp_basename = 'YMPdoc'

# -- Options for LaTeX output ---------------------------------------------

latex_elements = {
    'papersize': 'letterpaper',
    'pointsize': '10pt',
    'preamble': '',
    'figure_align': 'htbp',
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
    (master_doc, 'YMP.tex', 'YMP Documentation',
     'Elmar Pruesse', 'manual'),
]


# -- Options for manual page output ---------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    (master_doc, 'ymp', 'YMP Documentation',
     [author], 1)
]

# -- Options for Texinfo output -------------------------------------------

# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
    (master_doc, 'YMP', 'YMP Documentation',
     author, 'YMP', 'One line description of project.',
     'Miscellaneous'),
]

# Example configuration for intersphinx: refer to the Python standard library.
intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'snakemake': ('http://snakemake.readthedocs.io/en/stable', None),
    'sphinx': ('http://www.sphinx-doc.org/en/stable', None),
    'bioconda': ('https://bioconda.github.io', None),
    'click': ('http://click.pocoo.org/5/', None),
}


def run_apidoc(_):
    """
    Calls sphinx-apidoc to generate template .rst files for each module
    """
    from sphinx.ext import apidoc
    apidoc.OPTIONS = ['members', 'show-inheritance']
    apidoc.main(['--output-dir', docdir,
                 '--doc-project', 'API',
                 '--force',
                 '--module-first',
                 os.path.join(ympdir, 'ymp')])


def setup(app):
    app.connect('builder-inited', run_apidoc)
    app.add_stylesheet('custom.css')
