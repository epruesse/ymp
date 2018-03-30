"""Exceptions raised by YMP"""

from click import ClickException, echo
from inspect import stack


class YmpException(Exception):
    """Base class of all YMP Exceptions"""


class YmpNoStackException(YmpException, ClickException):
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
    just prefix the ``msg`` with ``Error: ``.
    """


class YmpRuleError(YmpNoStackException):
    """Indicates an error in the rules files

    This could e.g. be a Stage or Environment defined twice.

    Args:
      obj: The object causing the exception. Must have ``lineno``
        and ``filename`` as these will be shown as part of the error
        message on the command line.
      msg: The message to display
    """
    def __init__(self, obj: object, msg: str) -> None:
        self.obj = obj
        self.stack = stack()
        super().__init__(msg)

    def show(self) -> None:
        echo('Error in line %i of %s: %s' % (self.obj.lineno,
                                             self.obj.filename,
                                             self.format_message()),
             err=True)
        for fi in self.stack:
            if not fi.filename.endswith(".py"):
                echo(f"  included from {fi.filename}:{fi.lineno}")

