"""
YMP processes data in stages, each of which is contained in its own directory.

.. code-block:: snakemake

  with Stage("trim_bbmap") as S:
    S.doc("Trim reads with BBMap")
    rule bbmap_trim:
      output: "{:this:}/{sample}{:pairnames:}.fq.gz"
      input:  "{:prev:}/{sample}{:pairnames:}.fq.gz"
      ...

"""

from ymp.stage.stack import StageStack, find_stage

from ymp.stage.base import BaseStage, ConfigStage, Activateable
from ymp.stage.params import Param
from ymp.stage.stage import Stage
from ymp.stage.pipeline import Pipeline
from ymp.stage.reference import Reference
from ymp.stage.project import Project
from ymp.stage.expander import StageExpander
from ymp.stage.groupby import GroupBy
