Installing and Updating YMP
===========================


Install from GitHub
~~~~~~~~~~~~~~~~~~~

1. Clone the git repository::

      git clone https://github.com/epruesse/ymp.git

   Or, if your have github ssh keys set up::

      git clone git@github.com:epruesse/ymp.git

2. Create and activate a Conda environment for YMP::

      conda env create -n ymp --file environment.yaml
      source activate ymp

3. Install YMP into its Conda environment::

      pip install -e .

4. Verify that YMP works::

      source activate ymp
      ymp --help


Update GitHub Installation
~~~~~~~~~~~~~~~~~~~~~~~~~~

Usually, all you need to do is a pull::

   git pull

If you see errors before jobs are executed, the core requirements may
have changed. To update the YMP conda environment, enter the folder
where you installed YMP and run the following::

   source activate ymp
   conda env update --file environment.yaml

If something changed in ``setup.py``, a re-install may be necessary::

   source activate ymp
   pip install -U -e .

