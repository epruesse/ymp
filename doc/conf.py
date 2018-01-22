#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# YMP documentation build configuration file

# This file is execfile()d with the current directory set to its
# containing dir.

import os
import sys
sys.path.insert(0, os.path.abspath(os.pardir))

import ymp
import cloud_sptheme as csp

docdir = os.path.dirname(__file__)
ympdir = os.path.dirname(docdir)



# -- General configuration ------------------------------------------------

needs_sphinx = '1.6'


extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx',
    'sphinx.ext.todo',
    'sphinx.ext.coverage',
    'sphinx.ext.mathjax',
    'sphinx.ext.ifconfig',
    'sphinx.ext.viewcode',
    'sphinxcontrib.napoleon',
    'sphinxcontrib.fulltoc',
    'sphinx_click.ext',
    'ymp.sphinxext',
    'cloud_sptheme.ext.issue_tracker',
    'cloud_sptheme.ext.autodoc_sections',
    'cloud_sptheme.ext.relbar_links',
    'cloud_sptheme.ext.escaped_samp_literals',
    'cloud_sptheme.ext.issue_tracker',
    'cloud_sptheme.ext.table_styling',
]

templates_path = ['_templates']
html_static_path = ['_static']
source_suffix = ['.rst']
master_doc = 'contents'  # master toc
index_doc = 'index'  # frontpage

project = 'YMP Extensible Omics Pipeline'
author = 'Elmar Pruesse'
copyright = '2017-2018, ' + author

version = ymp.__version__
release = ymp.__release__

language = None

exclude_patterns = [
    '_build',
    'Thumbs.db',
    '.DS_Store'
]

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
    'https://docs.python.org/3/': None
    # 'snakemake': ('http://snakemake.readthedocs.org/', None),
}


def run_apidoc(_):
    """
    Calls sphinx-apidoc to generate template .rst files for each module
    """
    from sphinx.apidoc import main
    main(['',
          '--output-dir', docdir,
          '--doc-project', 'API',
          '--force',
          # '--no-toc',
          # '--no-headings',
          '--separate',
          '--module-first',
          os.path.join(ympdir, 'ymp')])


def setup(app):
    app.connect('builder-inited', run_apidoc)
