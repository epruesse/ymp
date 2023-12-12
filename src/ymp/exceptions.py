"""Exceptions raised by YMP"""
import sys
import textwrap
from inspect import stack
from typing import Optional, Tuple

from click import ClickException, echo

from snakemake.exceptions import WorkflowError, RuleException

class YmpException(Exception):
    """Base class of all YMP Exceptions"""


class YmpPrettyException(YmpException, ClickException, WorkflowError):
    """Exception that does not lead to stack trace on CLI

    Inheriting from ClickException makes ``click`` print only the
    ``self.msg`` value of the exception, rather than allowing Python
    to print a full stack trace.

    This is useful for exceptions indicating usage or configuration
    errors. We use this, instead of `click.UsageError` and friends so
    that the exceptions can be caught and handled explicitly where
    needed.

    Note that click will call the ``show`` method on this object to
    print the exception. The default implementation from click will
    just prefix the ``msg`` with ``Error:``.

    FIXME: This does not work if the exception is raised from within
        the snakemake workflow as snakemake.snakemake catches and
        reformats exceptions.
    """
    rule = None
    snakefile = None

class YmpLocateableError(YmpPrettyException):
    """Errors that have a file location to be shown

    Args:
      obj: The object causing the exception. Must have ``lineno``
        and ``filename`` as these will be shown as part of the error
        message on the command line.
      msg: The message to display
      show_includes: Whether or not the "stack" of includes should be printed.
    """
    def __init__(self, obj: object, msg: str, show_includes: bool = True) -> None:
        self.obj = obj
        self.stack = stack() if show_includes else []
        super().__init__(msg)

    def get_fileline(self) -> Tuple[str, int]:
        """Retrieve filename and linenumber from object associated with exception

        Returns:
           Tuple of filename and linenumber
        """
        return getattr(self.obj, "filename"), getattr(self.obj, "lineno")

    def show(self, file=None) -> None:
        super().show(file)
        if file is None:
            file = sys.stderr
        fname, line = self.get_fileline()
        if fname:
            if line is None:
                echo(f"Problem occurred in {fname}:", file=file)
            else:
                echo(f"Problem occurred in line {line} of {fname}:", file=file)
        for entry in self.stack:
            if not entry.filename.endswith(".py") and not entry.filename.endswith("/ymp"):
                echo(f"  while processing {entry.filename}:{entry.lineno}", file=file)


class YmpUsageError(YmpPrettyException):
    """General usage error"""


class YmpSystemError(YmpPrettyException):
    """Indicates problem running YMP with available system software"""


class YmpRuleError(YmpLocateableError):
    """Indicates an error in the rules files

    This could e.g. be a Stage or Environment defined twice.
    """


class YmpConfigError(YmpLocateableError):
    """Indicates an error in the ymp.yml config files

    Args:
      obj: Subtree of config causing error
      msg: The message to display
      key: Key indicating part of ``obj`` causing error
      exc: Upstream exception causing error
    """
    def __init__(self, obj: object, msg: str, key: Optional[object]=None) -> None:
        super().__init__(obj, msg)
        self.key = key

    def get_fileline(self):
        return self.obj.get_fileline(self.key)


class YmpStageError(YmpPrettyException):
    """Indicates an error in the requested stage stack
    """
    def __init__(self, msg: str) -> None:
        super().__init__(textwrap.dedent(msg))

    def show(self, file=None) -> None:
        echo(self.format_message(), err=True)


class YmpWorkflowError(YmpPrettyException):
    """Indicates an error during workflow execution

    E.g. failures to expand dynamic variables
    """
