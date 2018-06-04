YMP - a Flexible Omics Pipeline
===============================


|CircleCI| |Read the Docs| |Codacy grade| |Codecov|

.. |CircleCI| image:: https://img.shields.io/circleci/project/github/epruesse/ymp.svg?label=CircleCI
   :target: https://circleci.com/gh/epruesse/ymp
.. |Read the Docs| image:: https://img.shields.io/readthedocs/ymp/latest.svg
   :target: https://ymp.readthedocs.io/en/latest
.. |Codacy grade| image:: https://img.shields.io/codacy/grade/07ec32ae80194ec8b9184e1f6b5e6649.svg
   :target: https://app.codacy.com/app/elmar/ymp
.. |Codecov| image:: https://img.shields.io/codecov/c/github/epruesse/ymp.svg
   :target: https://codecov.io/gh/epruesse/ymp

.. begin intro

YMP is a tool that makes it easy to process large amounts of NGS read
data. It comes "batteries included" with everything needed to
preprocess your reads (QC, trimming, contaminant removal), assemble
metagenomes, annotate assemblies, or assemble and quantify RNA-Seq
transcripts, offering a choice of tools for each of those procecssing
stages. When your needs exceed what the stock YMP processing stages
provide, you can easily add your own, using YMP to drive novel tools,
tools specific to your area of research, or tools you wrote yourself.

.. end intro

:Note:
    Intrigued, but think YMP doesn't exactly fit your needs?

    Missing processing stages for your favorite tool? Found a bug?

    Open an issue, create a PR, or better yet, join the team!
   
The `YMP documentation <http://ymp.readthedocs.io/>`__ is available at
readthedocs.

.. begin features

Features:
---------

batteries included
  YMP comes with a large number of *Stages* implementing common read
  processing steps. These stages cover the most common topics,
  including quality control, filtering and sorting of reads, assembly
  of metagenomes and transcripts, read mapping, community profiling,
  visualisation and pathway analysis.

  For a complete list, check the `documentation
  <http://ymp.readthedocs.io/en/latest/stages.html>`__ or the `source
  <https://github.com/epruesse/ymp/tree/development/src/ymp/rules>`__.

get started quickly
  Simply point YMP at a folder containing read files, at a mapping
  file, a list of URLs or even an SRA RunTable and YMP will configure
  itself. Use tab expansion to complete your desired series of stages
  to be applied to your data. YMP will then proceed to do your
  bidding, downloading raw read files and reference databases as
  needed, installing requisite software environments and scheduling
  the execution of tools either locally or on your cluster.

explore alternative workflows
  Not sure which assembler works best for your data, or what the
  effect of more stringent quality trimming would be? YMP is made for
  this! By keeping the output of each stage in a folder named to match
  the stack of applied stages, YMP can manage many variant workflows
  in parallel, while minimizing the amount of duplicate computation
  and storage.

go beyond the beaten path
  Built on top of Bioconda_ and Snakemake_, YMP is easily extended with
  your own Snakefiles, allowing you to integrate any type of
  processing you desire into YMP, including your own, custom made
  tools. Within the YMP framework, you can also make use of the
  extensions to the Snakemake language provided by YMP (default
  values, inheritance, recursive wildcard expansion, etc.), making
  writing rules less error prone and repetative.

.. _Snakemake: https://snakemake.readthedocs.io
.. _Bioconda: https://bioconda.github.io
  
.. end features

.. begin background

Background
----------

Bioinformatical data processing workflows can easily get very complex,
even convoluted. On the way from the raw read data to publishable
results, a sizeable collection of tools needs to be applied,
intermediate outputs verified, reference databases selected, and
summary data produced. A host of data files must be managed, processed
individually or aggregated by host or spatial transect along the way.
And, of course, to arrive at a workflow that is just right for a
particular study, many alternative workflow variants need to be
evaluated. Which tools perform best? Which parameters are right?  Does
re-ordering steps make a difference? Should the data be assembled
individually, grouped, or should a grand co-assembly be computed?
Which reference database is most appropriate?

Answering these questions is a time consuming process, justifying the
plethora of published ready made pipelines each providing a polished
workflow for a typical study type or use case. The price for the
convenience of such a polished pipeline is the lack of flexibility -
they are not meant to be adapted or extended to match the needs of a
particular study. Workflow management systems on the other hand offer
great flexibility by focussing on the orchestration of user defined
workflows, but typicially require significant initial effort as they
come without predefined workflows.

YMP strives to walk the middle ground between these. It brings
everything needed to classic metagenome and RNA-Seq workflows, yet
built on the workflow management system Snakemake_, it can be easily
expanded by simply adding Snakemake rules files. Designed around the
needs of processing primarily multi-omic NGS read data, it brings a
framework for handling read file meta data, provisioning reference
databases, and organizing rules into semantic stages.

.. _Snakemake: https://snakemake.readthedocs.io

.. end background

.. begin developer info

Working with the Github Development Version
-------------------------------------------


Installing from GitHub
~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Clone the repository::

      git clone  --recurse-submodules https://github.com/epruesse/ymp.git
      
   Or, if your have github ssh keys set up::

      git clone --recurse-submodules git@github.com:epruesse/ymp.git

2. Create and activate conda environment::

      conda env create -n ymp --file environment.yaml
      source activate ymp

3. Install YMP into conda environment::
   
      pip install -e .

4. Verify that YMP works::

      source activate ymp
      ymp --help


Updating Development Version
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Usually, all you need to do is a pull::
   
  git pull
  git submodule update --recursive --remote

If environments where updated, you may want to regenerate the local
installations and clean out environments no longer used to save disk
space::

   source activate ymp
   ymp env update
   ymp env clean
   # alternatively, you can just delete existing envs and let YMP
   # reinstall as needed:
   # rm -rf ~/.ymp/conda*
   conda clean -a

If you see errors before jobs are executed, the core requirements may
have changed. To update the YMP conda environment, enter the folder
where you installed YMP and run the following::

  source activate ymp
  conda env update --file environment.yaml
  
If something changed in ``setup.py``, a re-install may be necessary::

   source activate ymp
   pip install -U -e .

.. end developer info
