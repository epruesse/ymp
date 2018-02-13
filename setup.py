#!/usr/bin/env python3

from setuptools import setup, find_packages


setup(
    name="YMP",
    use_scm_version=True,
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
        'networkx',
        'pygraphviz',
        'pytest',
        'yappi',
        'xlrd'
    ],
    install_requires=[
        'snakemake',
        'Click',
        'PyYAML',
        'drmaa',
        'rpy2',
        'pandas>=0.20',
        'coloredlogs'
    ],

    entry_points='''
        [console_scripts]
        ymp=ymp.cli:main
    '''
)
