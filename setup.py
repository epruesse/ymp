#!/usr/bin/env python3

import fastentrypoints  # NOQA pylint: disable=unused-import
from setuptools import setup, find_packages

setup(
    name="YMP",
    use_scm_version={'write_to': 'ymp/_version.py'},
    #description=,
    #long_description=,
    #long_description_content_type='text/x-rst', test/markdown
    url="https://github.com/epruesse/ymp",
    author="Elmar Pruesse",
    author_email="elmar.pruesse@ucdenver.edu",
    license="GPL-3",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 3.6',
    ],
    keywords="bioinformatics pipeline rnaseq metagenomics",
    project_urls={
        'Documentation': 'https://ymp.readthedocs.io',
        'Source': 'https://github.com/epruesse/ymp',
    }
    packages=find_packages(exclude=['docs', 'tests']),
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
    python_requires='>=3.6',
    include_package_data=True,
    entry_points='''
        [console_scripts]
        ymp=ymp.cli:main
    ''',
)
