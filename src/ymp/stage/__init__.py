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

import logging
import re
from typing import TYPE_CHECKING
from copy import copy

import ymp
from ymp.exceptions import YmpException, YmpRuleError, YmpStageError
from ymp.snakemake import ColonExpander, ExpandLateException, WorkflowObject


if TYPE_CHECKING:
    from typing import List
    from snakemake.rules import Rule

from ymp.stage.stack import StageStack
from ymp.stage.stage import Stage
from ymp.stage.pipeline import Pipeline
from ymp.stage.reference import Reference
from ymp.stage.project import Project
from ymp.stage.expander import StageExpander

