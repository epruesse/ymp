"""
This module manages the conda environments.
"""

import logging
import os
import os.path as op
import subprocess
from glob import glob
from typing import Union, Optional, Dict

import snakemake

import ymp
from ymp.common import ensure_list
from ymp.snakemake import BaseExpander
from ymp.exceptions import YmpException


log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class YmpEnvError(YmpException):
    """Failure in env"""


class MetaEnv(type):
    """Metaclass containing class methods and properties for `Env`

    The metaclass is separated out primarily to allow class property
    methods.
    """
    _CFG = None
    _ICFG = None
    _ENVS: Dict[str, str] = {}
    _STATIC_ENVS = None

    @property
    def icfg(cls):
        from ymp.config import icfg
        return icfg

    @property
    def cfg(cls):
        """Conda section of config (autloaded)

        Also performs tilde expansion on paths.
        """
        return cls.icfg.conda

    def new(cls, name: str, packages: Union[list, str], base: str="none",
            channels: Optional[Union[list, str]]=None):
        """Creates an inline defined conda environment

        Args:
          name: Name of conda environment (and basename of file)
          packages: package(s) to be installed into environment. Version
            constraints can be specified in each package string separated from
            the package name by whitespace. E.g. ``"blast =2.6*"``
          channels: channel(s) to be selected for the environment
          base: Select a set of default channels and packages to be added to
            the newly created environment. Sets are defined in conda.defaults
            in ``yml.yml``
        """

        with cls.cfg.defaults[base] as defaults:
            defaults.dependencies.extend(ensure_list(packages))
            defaults.channels.extend(ensure_list(channels))
            contents = f"name: {name}\n{defaults}"
            fname = op.join(cls.icfg.absdir.dynamic_envs,
                            f"{name}.yml")
            with open(fname, "w") as envf:
                envf.write(contents)
                cls._ENVS[name] = fname

    def get_installed_env_hashes(cls):
        return [
            dentry.name
            for dentry in os.scandir(cls.icfg.absdir.conda_prefix)
            if dentry.is_dir()
        ]

    def _get_builtin_static_envs(cls):
        for fname in glob(op.join(ymp._rule_dir, "*.yml")):
            yield Env(fname)

    def _get_dynamic_envs(cls):
        for fname in cls._ENVS.values():
            yield Env(fname)

    def get_envs(cls, static=True, dynamic=True):
        if static:
            yield from cls._get_builtin_static_envs()
        if dynamic:
            yield from cls._get_dynamic_envs()


class Env(snakemake.conda.Env, metaclass=MetaEnv):
    """Represents YMP conda environment

    Snakemake expects the conda environments in a per-workflow
    directory configured by ``conda_prefix``. YMP sets this value by
    default to ``~/.ymp/conda``, which has a greater chance of being
    on the same file system as the conda cache, allowing for hard
    linking of environment files.

    Within the folder ``conda_prefix``, each environment is created in
    a folder named by the hash of the environment definition file's
    contents and the ``conda_prefix`` path. This class inherits from
    ``snakemake.conda.Env`` to ensure that the hash we use is
    identical to the one Snakemake will use during workflow execution.

    The class provides additional features for updating environments,
    creating environments dynamically and executing commands within
    those environments.

    Note:
      This is not called from within the execution. Snakemake instanciates
      its own Env object purely based on the filename.

    """

    def __init__(self, env_file):
        # We initialize ourselves, rather than referring to super(),
        # because we lack a snakemake persistance dag object required
        # by snakemake.conda.Env to initialize _env_dir and _env_archive_dir

        self.file = env_file
        self.name, _ = op.splitext(op.basename(env_file))
        self._env_dir = Env.icfg.absdir.conda_prefix
        self._env_archive_dir = Env.icfg.absdir.conda_archive_prefix
        self._hash = None
        self._content_hash = None
        self._content = None
        self._path = None
        self._archive_file = None
        self._singularity_img = None

    def create(self):
        """Create conda environment""

        Inherits from snakemake.conda.Env.create
        """
        log.warning("Creating environment '%s'", self.name)
        return super().create()

    @property
    def installed(self):
        return op.isdir(self.path)

    def update(self):
        "Update conda environment"
        self.create()  # call create to make sure environment exists
        log.warning("Updating environment '%s'", self.name)
        return subprocess.run([
            "conda", "env", "update",
            "--prune",
            "-p", self.path,
            "-f", self.file,
            "-v"
        ]).returncode

    def run(self, command):
        """Execute command in environment

        Returns exit code of command run.
        """
        return subprocess.run(
            "$SHELL -c '. activate {}; {}'"
            "".format(self.path, " ".join(command)),
            shell=True).returncode

    def __lt__(self, other):
        "Comparator for sorting"
        return self.name < other.name


class CondaPathExpander(BaseExpander):
    """Applies search path for conda environment specifications

    File names supplied via `rule: conda: "some.yml"` are replaced with
    absolute paths if they are found in any searched directory.
    Each `search_paths` entry is appended to the directory
    containing the top level Snakefile and the directory checked for
    the filename. Thereafter, the stack of including Snakefiles is traversed
    backwards. If no file is found, the original name is returned.
    """
    def __init__(self, config, *args, **kwargs):
        try:
            from snakemake.workflow import workflow
            self._workflow = workflow
            super().__init__(*args, **kwargs)
        except ImportError:
            log.debug("CondaPathExpander not registered -- needs workflow")

        self._search_paths = Env.cfg.env_path

    def expands_field(self, field):
        return field == 'conda_env'

    def format(self, conda_env, *args, **kwargs):
        for snakefile in reversed(self._workflow.included_stack):
            basepath = op.dirname(snakefile)
            for _, relpath in sorted(self._search_paths.items()):
                searchpath = op.join(basepath, relpath)
                abspath = op.abspath(op.join(searchpath, conda_env))
                for ext in "", ".yml", ".yaml":
                    if op.exists(abspath+ext):
                        return abspath+ext
        return conda_env


