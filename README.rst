YMP - a Flexible Omics Pipeline
===============================

|CircleCI| |Read the Docs| |Codacy grade| |Codecov|

YMP is a tool that makes it easy to process large amounts of NGS read
data. It comes "batteries included" with everything needed to
preprocess your reads (QC, trimming, contaminant removal), assemble
metagenomes, annotate assemblies, or assemble and quantify RNA-Seq
transcripts, offering a choice of tools for each of those procecssing
stages. When your needs exceed what the stock YMP processing stages
provide, you can easily add your own, using YMP to drive novel tools,
tools specific to your area of research, or tools you wrote yourself.

:Note:
    Intrigued, but think YMP doesn't exactly fit your needs?

    Missing processing stages for your favorite tool? Found a bug?

    Open an issue, create a PR, or better yet, join the team!


The `YMP documentation <http://ymp.readthedocs.io/>`__ is available at
readthedocs.


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
  Built on top of Bioconda and Snakemake, YMP is easily extended with
  your own Snakefiles, allowing you to integrate any type of
  processing you desire into YMP, including your own, custom made
  tools. Within the YMP framework, you can also make use of the
  extensions to the Snakemake language provided by YMP (default
  values, inheritance, recursive wildcard expansion, etc.), making
  writing rules less error prone and repetative.


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
built on the workflow management system Snakemake, it can be easily
expanded by simply adding Snakemake rules files. Designed around the
needs of processing primarily multi-omic NGS read data, it brings a
framework for handling read file meta data, provisioning reference
databases, and organizing rules into semantic stages.

FIXME: finish :)


Working with the Github Development Version
-------------------------------------------

1. Installing YMP from git
~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Clone the repository:

   .. code:: shell

      git clone --recurse-submodules git@github.com:epruesse/ymp.git
      
      # or, if you have an SSH key set up:
      # git clone  --recurse-submodules https://github.com/epruesse/ymp.git

2. Create and activate conda environment:
   
   .. code:: shell

      conda env create -n ymp --file environment.yaml
      source activate ymp

3. Install YMP into conda environment
   
   .. code:: shell

      pip install -e .

4. Verify all is ok
   
   .. code:: shell

      source activate ymp
      ymp --help``

2. Updating YMP installed from git
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Usually, all you need to do is a pull:

.. code:: shell
   
   git pull

If you see errors before jobs are executed, the core requirements may
have changed. Try updating the conda environment:

.. code:: shell
	  
   source activate ymp
   conda env update --file environment.yaml
  
If something changed in ``setup.py``, a re-install may be necessary:

.. code:: shell

   source activate ymp
   pip install -U -e .


.. |CircleCI| image:: https://img.shields.io/circleci/project/github/epruesse/ymp.svg?label=CircleCI
   :target: https://circleci.com/gh/epruesse/ymp
.. |Read the Docs| image:: https://img.shields.io/readthedocs/ymp/latest.svg
   :target: https://ymp.readthedocs.io/en/latest
.. |Codacy grade| image:: https://img.shields.io/codacy/grade/07ec32ae80194ec8b9184e1f6b5e6649.svg
   :target: https://app.codacy.com/app/elmar/ymp
.. |Codecov| image:: https://img.shields.io/codecov/c/github/epruesse/ymp.svg
   :target: https://codecov.io/gh/epruesse/ymp
