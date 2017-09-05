# ymp - flexible omics pipeline

[![CircleCI](https://img.shields.io/circleci/project/github/epruesse/ymp.svg?label=CircleCI)](https://circleci.com/gh/epruesse/ymp)
[![Travis](https://img.shields.io/travis/epruesse/ymp.svg?label=TravisCI)](https://travis-ci.org/epruesse/ymp)

## Install from github

1. Check out repo
  ```
  git clone https://github.com/epruesse/ymp.git
  ```
  or
  ```
  git clone git@github.com:epruesse/ymp.git
  ```

2. Create and activate conda environment
  ```
  conda env create -n ymp -f environment.yaml
  source activate ymp
  ```

3. Install Ymp into conda environment
  ```
  pip install -e .
  ```

4. Run Ymp
  ```
  source activate ymp
  ymp --help
  ```

5. Update Ymp
  ```
  git pull
  conda env update -f environment.yaml # usually not necessary
  ```
