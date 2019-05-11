#!/usr/bin/env python3

import fastentrypoints  # NOQA pylint: disable=unused-import
from setuptools import setup, find_packages


def read_file(fn):
    with open(fn) as f:
        content = f.read()
    return content


def get_content_type(fn):
    if fn.endswith(".md"):
        return "text/markdown"
    if fn.endswith(".rst"):
        return "text/x-rst"
    return "text/plain"


setup(
    name="ymp",
    use_scm_version={'write_to': 'src/ymp/_version.py'},
    description="Flexible multi-omic pipeline system",
    long_description=read_file("README.rst"),
    long_description_content_type=get_content_type("README.rst"),
    url="https://github.com/epruesse/ymp",
    author="Elmar Pruesse",
    author_email="elmar.pruesse@ucdenver.edu",
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
    keywords=("bioinformatics pipeline rnaseq metagenomics "
              "conda bioconda snakemake"),
    project_urls={
        'Documentation': 'https://ymp.readthedocs.io',
        'Source': 'https://github.com/epruesse/ymp',
    },
    packages=find_packages('src'),
    package_dir={'': 'src'},
    zip_safe=False,
    setup_requires=[
        'setuptools_scm>=3.2',
        'pytest-runner',
    ],
    tests_require=[
        'pytest-xdist',
        'pytest-logging',
        'pytest-timeout',
        'pygraphviz',
        'pytest',
        'yappi',
        'xlrd',
    ],
    install_requires=[
        'snakemake>=5.4.4',
        'Click',
        'Click-completion',
        'ruamel.yaml>0.15',
        'drmaa',
        'pandas>=0.20',
        'networkx>=2',
        'coloredlogs',
        'xdg',
        'aiohttp',
        'tqdm>=4.21.0',
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
    python_requires='>=3.6',
    include_package_data=True,
    entry_points='''
        [console_scripts]
        ymp=ymp.cli:main
    ''',
)
