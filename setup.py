#!/usr/bin/env python3

import fastentrypoints  # NOQA pylint: disable=unused-import
from setuptools import setup, find_packages


def read_file(fn):
    with open(fn) as f:
        content = f.read()
    return content

setup(
    name="ymp",
    use_scm_version={'write_to': 'src/ymp/_version.py'},
    description="Flexible multi-omic pipeline system",
    long_description=read_file("README.rst"),
    long_description_content_type="text/x-rst",
    url="https://github.com/epruesse/ymp",
    author="Elmar Pruesse",
    author_email="elmar@pruesse.net",
    license="GPL-3",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Natural Language :: English',
        'Operating System :: MacOS',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.6',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
    ],
    platforms=["linux", "macos"],
    keywords=("bioinformatics pipeline workflow automation "
              "rnaseq genomics metagenomics "
              "conda bioconda snakemake"),
    project_urls={
        'Documentation': 'https://ymp.readthedocs.io',
        'Source': 'https://github.com/epruesse/ymp',
    },
    packages=find_packages('src'),
    package_dir={'': 'src'},
    zip_safe=False,
    setup_requires=[
        'setuptools_scm>=3.4',
        'setuptools>=42',
        'wheel',
        'pytest-runner',
    ],
    install_requires=[
        'snakemake>=7.32,<8',
        'click>8',
        'click-completion',
        'ruamel.yaml>0.15',
        'drmaa',
        'pandas>=0.20',
        'openpyxl',  # excel support
        'coloredlogs',
        'xdg',  # user paths
        'aiohttp',
        'tqdm>=4.21.0',
    ],
    tests_require=[
        'networkx>=2',
        'pytest-xdist',
        'pytest-logging',
        'pytest-timeout',
        'pygraphviz',
        'pytest',
        'yappi',
        'pytest-cov',
        'codecov'
    ],
    extras_require={
        'docs': [
            'sphinx',
            'cloud_sptheme',
            'sphinxcontrib-fulltoc',
            'sphinx-click',
            'sphinx_autodoc_typehints',
            'ftputil',
        ]
    },
    python_requires='>=3.10',
    include_package_data=True,
    entry_points='''
        [console_scripts]
        ymp=ymp.cli:main
    ''',
)
