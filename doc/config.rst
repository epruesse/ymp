Configuration
=============

YMP reads its configuration from a YAML formatted file ``ymp.yml``. To
run YMP, you need to first tell it which datasets you want to process
and where it can find them.

.. contents:: Contents
   :local:

Getting Started
---------------

A simple configuration looks like this:

.. code:: yaml

    projects:
      myproject:
        data: mapping.csv

This tells YMP to look for a file ``mapping.csv`` located in the same
folder as your ``ymp.yml`` listing the datasets for the project
``myproject``. By default, YMP will use the left most unique column as
names for your datasets and try to guess which columns point to your
input data.

The matching ``mapping.csv`` might look like this:

.. code:: text

    sample,fq1,fq2
    foot,sample1_1.fq.gz,sample1_2.fq.gz
    hand,sample2_1.fq,gz,sample2_2.fq.gz

So we have two samples, ``foot`` and ``hand``, and the read files for
those in the same directory as the configuration file. Using relative or
absolute paths you can point to any place in your filesystem. You can
also use SRA references like ``SRR123456`` or URLs pointing to remote
files.

The mapping file itself may be in comma separated or tab separated
format or may be an Excel file. For Excel files, you may specify the
sheet to be used separated from the file name by a ``%`` sign. For
example:

.. code:: yaml

    project:
      myproject:
        data: myproject.xlsx%sheet3

The matching Excel file could then have a ``sheet3`` with this content:

  +----------+-------------------------------+-------------------------------+-------------+
  | sample   | fq1                           | fq2                           | srr         |
  +==========+===============================+===============================+=============+
  | foot     | /data/foot1.fq.gz             | /data/foot2.fq.gz             |             |
  +----------+-------------------------------+-------------------------------+-------------+
  | hand     |                               |                               | SRR123456   |
  +----------+-------------------------------+-------------------------------+-------------+
  | head     | http://datahost/head1.fq.gz   | http://datahost/head2.fq.gz   | SRR234234   |
  +----------+-------------------------------+-------------------------------+-------------+

For ``foot``, the two gzipped FastQ files are used. The data for
``hand`` is retrieved from SRA and the data for ``head`` downloaded from
``datahost``. The SRR number for ``head`` is ignored as the URL pair is
found first.

Referencing Read Files
----------------------

YMP will search your map file data for references to the read data
files. It understands three types of references to your reads:

Local FastQ files: ``data/some_1.fq.gz, data/some_2.fq.gz``
   The file names should end in ``.fastq`` or ``.fq``, optionally followed
   by ``.gz`` if your data is compressed. You need to provide forward and
   reverse reads in separate columns; the left most column is assumed to
   refer to the forward reads.

   If the filename is relative (does not start with a ``/``), it is assumed
   to be relative to the location of ``ymp.yml``.

Remote FastQ files: ``http://myhost/some_1.fq.gz, http://myhost/some_2.fq.gz``
   If the filename starts with ``http://`` or ``https://``, YMP will
   download the files automatically.

   Forward and reverse reads need to be either both local or both remote.

SRA Run IDs: ``SRR123456``
   Instead of giving names for FastQ files, you may provide SRA Run
   accessions, e.g. ``SRR123456`` (or ``ERRnnn`` or ``DRRnnn`` for runs
   originally submitted to EMBL or DDBJ, respectively). YMP will use
   ``fastq-dump`` to download and extract the SRA files.

Which type to use is determined for each row in your map file data
individually. From left to right, the first recognized data source is
used in the order they are listed above.

Configuration processing an SRA RunTable:

   .. code:: yaml

      projects:
        smith17:
          data:
            - SraRunTable.txt
          id_col: Sample_Name_s

Project Configuration
---------------------

Each project must have a ``data`` key defining which mapping file(s) to
load. This may be a simple string referring to the file (URLs are OK as
well) or a more `complex
configuration <#multiple-mapping-files-per-project>`__.

Specifying Columns
~~~~~~~~~~~~~~~~~~

By default, YMP will choose the columns to use as data set name and to
locate the read data automatically. You can override this behavior by
specifying the columns explicitly:

1. Data set names: ``id_col: Sample``

   The left most unique column may not always be the most informative to
   use as names for the datasets. In the above example, we specify the
   column to use explicitly with the line ``id_col: Sample_Name_s`` as the
   columns in SRA run tables are sorted alpha-numerically and the left most
   unique one may well contain random numeric data.

   Default: left most unique column

2. Data set read columns: ``reads_cols: [fq1, fq2]``

   If your map files contain multiple references to source files, e.g.
   local and remote, and the order of preference used by YMP does not meet
   your needs you can restrict the search for suitable data references to a
   set of columns using the key ``read_cols``.

   Default: all columns

Example
'''''''

.. code:: yaml

    projects:
      smith17:
        data:
          - SraRunTable.txt
        id_col: Sample_Name_s
        read_cols: Run_s

Multiple Mapping Files per Project
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To combine data sets from multiple mapping files, simply list the files
under the ``data`` key:

.. code:: yaml

    projects:
      myproject:
        data:
          - sequencing_run_1.txt
          - sequencing_run_2.txt

The files should at least share one column containing unique values to
use as names for the datasets.

If you need to merge meta-data spread over multiple files, you can use
the ``join`` key:

.. code:: yaml

    project:
      myproject:
        data:
          - join:
              - SraRunTable.txt
              - metadata.xlsx%reference_project
          - metadata.xlsx%our_samples

This will merge rows from ``SraRunTable.txt`` with rows in the
``reference_project`` sheet in ``metadata.xls`` if all columns of the
same name contain the same data (natural join) and add samples from the
``our_samples`` sheet to the bottom of the list.

Complete Example
~~~~~~~~~~~~~~~~

.. code:: yaml

    projects:
      myproject:
        data:
          - join:
              - SraRunTable.txt
              - metadata.xlsx%reference_project
          - metadata.xlsx%our_samples
          - mapping.csv
        id_col: Sample
        read_cols:
          - fq1
          - fq2
          - Run_s
