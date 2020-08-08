import logging
from typing import Set


log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class BaseStage(object):
    """Base class for stage types"""
    def __init__(self, name: str) -> None:
        self.name = name
        self.docstring: str = None

    def __str__(self) -> str:
        return self.name

    def __repr__(self):
        return f"{self.__class__.__name__}({self!s})"

    def doc(self, doc: str) -> None:
        """Add documentation to Stage

        Args:
          doc: Docstring passed to Sphinx
        """
        #: Docstring describing stage
        self.docstring = doc
        
        #: Outputs provided by stage
        self.outputs: Set[str] = set()

    def match(self, name: str) -> bool:
        """Check if the ``name`` can refer to this stage

        This is a function because stages can have multiple names
        parameters conveyed by modifying the name.
        """
        return name == self.name

    def get_inputs(self) -> Set[str]:
        """Returns the set of inputs required by this stage

        Function returns a copy, to ensure internal data is not modified.
        """
        return set()

    def can_provide(self, inputs: Set[str]) -> Set[str]:
        """Determines which of ``inputs`` this stage can provide"""
        return inputs.intersection(self.outputs)

    def get_path(self):
        """On disk location for this stage.

        Returns:
          Path to fixed location for this stage or None.
        """
        return None


class ConfigStage(BaseStage):
    """Base for stages created via configuration

    These Stages derive from the ``yml.yml`` and not from a rules file.
    """
    def __init__(self, name: str, cfg: 'MultiProxy'):
        self.filename = ';'.join(cfg.get_files())
        self.lineno = next(iter(cfg.get_linenos()), None)
        super().__init__(name)
        self.cfg = cfg

    @property
    def defined_in(self):
        """List of files defining this stage
        
        Used to invalidate caches.
        """
        return self.cfg.get_files()
