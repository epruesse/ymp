"""
"""

import logging

from ymp.util import AttrDict
from ymp.exceptions import YmpException

log = logging.getLogger(__name__)

stage = AttrDict()


class YmpStageError(YmpException):
    def __init__(self, stage, msg):
        msg = "Error in stage '{}': {}".format(stage, msg)
        super().__init__(msg)


class Stage(object):
    """
    Creates an YMP Stage

    Example:
      with Stage("trim_bbmap") as S:
        S.doc("Trim reads with BBMap")
        rule bbmap_trim:
          output: "{:this:}/{sample}{:pairnames:}.fq.gz"
          input:  "{:prev:}/{sample}{:pairnames:}.fq.gz"
          ...
    """

    #: Currently active stage ("entered")
    active = None

    def __init__(self, name, *args):
        if name in stage:
            raise YmpStageError(name, "Duplicate stage name")

        self.name = name
        log.debug("New Stage '{}'".format(name))
        stage[name] = self

    def doc(self, doc):
        """Add documentation to Stage"""
        self.doc = doc

    def __enter__(self):
        if Stage.active is not None:
            raise YmpStageError(
                self.name,
                "Stage {} already active".format(self.active.name)
            )

        Stage.active = self
        return self

    def __exit__(self, *args):
        Stage.active = None
