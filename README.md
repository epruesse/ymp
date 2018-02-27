# ymp - flexible omics pipeline

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/07ec32ae80194ec8b9184e1f6b5e6649)](https://app.codacy.com/app/elmar/ymp?utm_source=github.com&utm_medium=referral&utm_content=epruesse/ymp&utm_campaign=badger)
[![CircleCI](https://img.shields.io/circleci/project/github/epruesse/ymp.svg?label=CircleCI)](https://circleci.com/gh/epruesse/ymp)
[![Read the Docs](https://img.shields.io/readthedocs/ymp/latest.svg)](https://ymp.readthedocs.io/en/latest)

YMP is an omics pipeline designed to simplify processing large numbers of
read files while remaining flexible and easily extented.

YMP relies upon Bioconda to provide all necessary tools in up-to-date versions
and Snakemake to handle task execution.

Please refer to the [manual](http://ymp.readthedocs.io/) for more help.

## Github development version

### 1. Install from github

1. Check out repo
  ```
  git clone https://github.com/epruesse/ymp.git
  ```
  or (requires ssh key set up)
  ```
  git clone git@github.com:epruesse/ymp.git
  ```

2. Create and activate conda environment for YMP
  ```
  conda env create -n ymp --file environment.yaml
  source activate ymp
  ```

3. Install YMP into conda environment
  ```
  pip install -e .
  ```

4. Check that Ymp runs
  ```
  source activate ymp
  ymp --help
  ```

### 2. Update github installation

Usually, all you need to do is a pull:

  ```
  git pull
  ```

If you see errors before jobs are executed, the core requirements may have changed.
Try updating the conda environment:

  ```
  source activate ymp
  conda env update --file environment.yaml
  ```
  
If something changed in `setup.py`, a re-install may be necessary:

  ```
  source activate ymp
  pip install -U -e . 
  ```


