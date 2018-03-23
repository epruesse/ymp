#!/usr/bin/env python3

import fastentrypoints  # NOQA pylint: disable=unused-import
from setuptools import setup, find_packages


setup(
    name="YMP",
    use_scm_version={'write_to': 'ymp/_version.py'},
    author="Elmar Pruesse",
    author_email="elmar.pruesse@ucdenver.edu",
    url="https://github.com/epruesse/ymp",
    #description=,
    #long_description=,
    #license=,
    #keywords=,

    packages=find_packages(),
    package_data={ '': [
        'rules/Snakefile',
        'rules/*.rules',
        'rules/*.yml',
        'etc/*.yml'
    ]},
    zip_safe=False,

    setup_requires=[
        'setuptools_scm',
        'pytest-runner'
    ],
    tests_require=[
        'pytest-xdist',
        'pytest-logging',
        'pytest-timeout',
        'pygraphviz',
        'pytest',
        'yappi',
        'xlrd'
    ],
    install_requires=[
        'snakemake',
        'Click',
        'Click-completion',
        'ruamel.yaml>0.15',
        'drmaa',
        'pandas>=0.20',
        'networkx>=2',
        'coloredlogs',
        'xdg'
    ],

    entry_points='''
        [console_scripts]
        ymp=ymp.cli:main
    '''
)
