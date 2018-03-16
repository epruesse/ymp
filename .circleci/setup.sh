#!/bin/bash
#
# Installs Bio-Conda and all dependencies
# - adds miniconda PATH to bashrc (for CircleCI)
# - manages a tarball if file `LOCAL` present (for ./cicleci build)
# - creates conda_state.txt for caching with circleci "{{checksum}}"
# - updates packages with every run
# - needs BASH_ENV to point to the bashrc
# - needs MINICONDA to point to the miniconda install path
set -x

CONDA_BASEURL=https://repo.continuum.io/miniconda

# expand '~' in MINICONDA path (alternatives to eval are too long)
eval MINICONDA=$MINICONDA

# Setup PATH
if test -n "$BASH_ENV"; then
    echo "Prepending $MINICONDA/bin to PATH in $BASH_ENV"
    cat >>$BASH_ENV <<EOF
if [ -z "\$CONDA_PATH_BACKUP" ]; then
    export PATH="$MINICONDA/bin:\$PATH"
fi
EOF
fi
export PATH="$MINICONDA/bin:$PATH"


# Install Miniconda if missing
if test -d $MINICONDA; then
    echo "Found conda install"
else
    # Determine OS
    case $(uname) in
	Linux)  CONDA_OSNAME=Linux;;
	Darwin) CONDA_OSNAME=MacOSX;;
    esac

    # Download and install miniconda
    curl $CONDA_BASEURL/Miniconda3-latest-$CONDA_OSNAME-x86_64.sh -o miniconda.sh
    bash miniconda.sh -b -p $MINICONDA
    hash -r

    # Configure
    conda config --system --set always_yes yes --set changeps1 no
    conda config --system --add channels defaults
    conda config --system --add channels conda-forge
    conda config --system --add channels bioconda

    # Install tools
    conda install git pip
    conda update -q conda git
fi


# Install/update YMP dependencies
if test -d $MINICONDA/envs/test_env; then
    conda env update -n test_env -f environment.yaml --prune
else
    conda env create -n test_env -f environment.yaml
fi

# Cleanup
conda clean --yes --all

# Dump status
mkdir -p conda
conda info > conda/info.txt
conda list > conda/root.txt
ls -1 $MINICONDA/pkgs > conda/pkgs.txt
ls -d1 ~/.ymp/conda/* > conda/ymp_envs.txt

# Exit ok
true



