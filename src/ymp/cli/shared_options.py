import logging
import os
import sys

import click
import tqdm
from coloredlogs import ColoredFormatter

log = logging.getLogger(__name__)  # pylint: disable=invalid-name

context_settings = {
    'help_option_names': ['-h', '--help']
}


class Group(click.Group):
    def command(self, *args, **kwargs):
        command = super().command(*args, context_settings=context_settings,
                                  **kwargs)

        def wrapper(f):
            return command(log_options(f))
        return wrapper


def command(*args, **kwargs):
    command = click.command(*args, context_settings=context_settings, **kwargs)

    def wrapper(f):
        return command(log_options(f))
    return wrapper


def group(*args, **kwargs):
    return command(*args, cls=Group, **kwargs)


def nohup(ctx, param, val):
    """
    Make YMP continue after the shell dies.

    - redirects stdout and stderr into pipes and sub process that won't
      die if it can't write to either anymore
    - closes stdin

    """
    if not val:
        return

    import signal
    from select import select
    from threading import Thread

    # ignore sighup
    signal.signal(signal.SIGHUP, signal.SIG_IGN)

    # sighup is actually only sent by bash if bash gets it itself, so not if
    # `exit` is called with ymp in the background. What usually kills that type
    # of process is trying to write to stdout or stderr. We need to catch that.

    # close stdin (don't need that anyway)
    sys.stdin.close()

    # redirect stdout and err into a pipe and save original target
    pipes = {}
    for fd in (sys.stdout, sys.stderr):
        # save original std fd
        saved = os.dup(fd.fileno())
        # create a pipe
        pipe = os.pipe()
        # overwrite std fd with one end of pipeo
        os.dup2(pipe[1], fd.fileno())
        # save other end and target std fd
        pipes[pipe[0]] = saved

    def watcher():
        while True:
            r, _, x = select(pipes.keys(), [], [], 60)
            for pipe in pipes:
                if pipe in r:
                    data = os.read(pipe, 4096)
                    try:
                        os.write(pipes[pipe], data)
                    except IOError:
                        pass

    t = Thread(target=watcher)
    t.daemon = True
    t.start()


nohup_option = click.option(
    "--nohup", "-N", is_flag=True,
    help="Don't die once the terminal goes away.",
    callback=nohup
)


class TqdmHandler(logging.StreamHandler):
    """Tqdm aware logging StreamHandler

    Passes all log writes through tqdm to allow progress bars
    and log messages to coexist without clobbering terminal
    """
    def emit(self, record):
        tqdm.tqdm.write(self.format(record))


class LogFormatter(ColoredFormatter):
    snakemake_level_styles = {
        'warning': {'color': 'yellow'},
        'info': {'color': 'green'},
        'debug': {'color': 'blue'},
        'crirical': {'color': 'red'},
        'error': {'color': 'red'},
    }
    def __init__(self):
        super().__init__("%(source)s%(message)s", level_styles=self.snakemake_level_styles)

    def format(self, record):
        if record.name.startswith('ymp.'):
            record.source = "YMP "
        else:
            record.source = ""
        return super().format(record)


class Log:
    """
    Set up Logging
    """
    # We could get snakemake's logging output like so:
    # slog = logging.getLogger("snakemake")
    # slog.parent = log
    #
    # Or use the log_handler parameter to snakemake to override
    # turning log events into messages. That would take some rewriting
    # of snakemake code though.
    #
    # There seems to be no good way of redirecting snakemakes logging
    # in a python-logging style way as it installs its own stream
    # handlers once control has passed to snakemake.
    def __init__(self):
        self.root_logger = logging.getLogger()
        self.log = logging.getLogger("ymp")
        self.log.setLevel(logging.WARNING)
        if not any(isinstance(handler, TqdmHandler) for handler in self.root_logger.handlers):
            self.console_handler = TqdmHandler()
            self.console_handler.setLevel(logging.DEBUG)  # no filtering
            self.console_handler.setFormatter(LogFormatter())
            self.root_logger.addHandler(self.console_handler)

    def mod_level(self, n):
        new_level = self.log.getEffectiveLevel() + n*10
        clamped_level = max(logging.DEBUG, min(logging.CRITICAL, new_level))
        self.log.setLevel(clamped_level)

    @staticmethod
    def set_logfile(filename):
        log_handler = logging.FileHandler(filename)
        log_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
        log_handler.setFormatter(formatter)
        logging.getLogger().addHandler(log_handler)

    @classmethod
    def verbose_option(cls, ctx, param, val):
        log = ctx.ensure_object(Log)
        if val:
            log.mod_level(-val)

    @classmethod
    def quiet_option(cls, ctx, param, val):
        log = ctx.ensure_object(Log)
        if val:
            log.mod_level(val)

    @classmethod
    def logfile_option(cls, ctx, param, val):
        log = ctx.ensure_object(Log)
        if val:
            log.set_logfile(val)


verbose_option = click.option(
    "--verbose", "-v", count=True,
    help="Increase log verbosity",
    callback=Log.verbose_option,
    expose_value=False
)


quiet_option = click.option(
    "--quiet", "-q", count=True,
    help="Decrease log verbosity",
    callback=Log.quiet_option,
    expose_value=False
)

logfile_option = click.option(
    "--log-file",
    help="Specify a log file",
    callback=Log.logfile_option,
    expose_value=False
)


def enable_debug(_ctx, param, val):
    import pdb  # pylint: disable=import-outside-toplevel
    import signal  # pylint: disable=import-outside-toplevel
    if not val:
        return

    def excepthook(typ, val, trace):
        import traceback  # pylint: disable=import-outside-toplevel
        traceback.print_exception(typ, val, trace)
        pdb.pm()

    sys.excepthook = excepthook
    log.error("Dropping into PDB on uncaught exception...")

    def handle_sigusr1(_sig, frame):
        pdb.Pdb().set_trace(frame)

    signal.signal(signal.SIGUSR1, handle_sigusr1)
    log.error("To trigger debugger immediately, call 'kill -s SIGUSR1 %i'",
              os.getpid())


debug_option = click.option(
    "--pdb", "-P", is_flag=True,
    help="Drop into debugger on uncaught exception",
    callback=enable_debug,
    expose_value=False
)


def log_options(f):
    f = logfile_option(f)
    f = verbose_option(f)
    f = quiet_option(f)
    f = debug_option(f)
    return f
