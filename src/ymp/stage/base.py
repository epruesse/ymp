import logging
import os

from typing import Set, Dict, Union, List


log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class BaseStage(object):
    """Base class for stage types"""
    #: The name of the stamp file that is touched to indicate
    #: completion of the stage.
    STAMP_FILENAME = "all_targets.stamp"
    def __init__(self, name: str) -> None:
        #: The name of the stage is a string uniquely identifying it
        #: among all stages.
        self.name = name

        #: The docstring describing this stage. Visible via ``ymp
        #:stage list`` and in the generated sphinx documentation.
        self.docstring: str = None

    def __str__(self) -> str:
        """Cast to string we just emit our name"""
        return self.name

    def __repr__(self):
        """Using `repr()` we emit the subclass as well as our name"""
        return f"{self.__class__.__name__}({self!s})"

    def doc(self, doc: str) -> None:
        """Add documentation to Stage

        Args:
          doc: Docstring passed to Sphinx
        """
        #: Docstring describing stage
        self.docstring = doc

    def match(self, name: str) -> bool:
        """Check if the ``name`` can refer to this stage

        As component of a `StageStack`, a stage may be identified by
        alternative names and may also be parametrized by suffix
        modifiers. Stage types supporting this behavior must override
        this function.

        """
        return name == self.name

    def get_inputs(self) -> Set[str]:
        """Returns the set of inputs required by this stage

        This function must return a copy, to ensure internal data is
        not modified.

        """
        return set()

    @property
    def outputs(self) -> Union[Set[str], Dict[str, str]]:
        """Returns the set of outputs this stage is able to generate.

        May return either a `set` or a `dict` with the dictionary
        values representing redirections in the case of virtual stages
        such as `Pipeline` or `Reference`.

        """
        return set()

    def can_provide(self, inputs: Set[str]) -> Dict[str, str]:
        """Determines which of ``inputs`` this stage can provide.

        Returns a dictionary with the keys a subset of ``inputs`` and
        the values identifying redirections. An empty string indicates
        that no redirection is to take place. Otherwise, the string is
        the suffix to be appended to the prior `StageStack`.

        """
        return {
            output: ""
            for output in inputs.intersection(self.outputs)
        }

    def get_path(self, stack: "StageStack") -> str:
        """On disk location for this stage given ``stack``.

        Called by `StageStack` to determine the real path for
        virtual stages (which must override this function).
        """
        return stack.name

    def get_all_targets(self, stack: "StageStack") -> List[str]:
        """Targets to build to complete this stage given ``stack``.

        Typically, this is the StageStack's path appended with the
        stamp name.
        """
        return [os.path.join(stack.path, self.STAMP_FILENAME)]


class ConfigStage(BaseStage):
    """Base for stages created via configuration

    These Stages derive from the ``yml.yml`` and not from a rules file.
    """
    def __init__(self, name: str, cfg: 'MultiProxy'):
        #: Semi-colon separated list of file names defining this Stage.
        self.filename = ';'.join(cfg.get_files())
        #: Line number within the first file at which this Stage is defined.
        self.lineno = next(iter(cfg.get_linenos()), None)
        super().__init__(name)
        #: The configuration object defining this Stage.
        self.cfg = cfg

    @property
    def defined_in(self):
        """List of files defining this stage
        
        Used to invalidate caches.
        """
        return self.cfg.get_files()
