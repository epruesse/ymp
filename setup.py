#!/usr/bin/env python3

from setuptools import setup, find_packages


setup(
    name="YMP",
    use_scm_version=True,
    packages=find_packages(),
    package_data={ '': [
        'rules/Snakefile',
        'rules/*.rules',
        'rules/*.yml',
        'etc/*.yml'
    ]},
    zip_safe=False,
    setup_requires=[
        'setuptools_scm'
    ],
    install_requires=[
        'snakemake',
        'Click',
        'PyYAML',
        'drmaa',
        'rpy2'
    ],
    entry_points='''
        [console_scripts]
        ymp=ymp.cmd:cli
    '''



    #author=,
    #author_email=,
    #description=,
    #license=,
    #keywords=,
    #url=,
)
