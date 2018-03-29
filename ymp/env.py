"""
This module manages the conda environments.
"""

import logging
import os
import os.path as op
import subprocess
from typing import Optional, Union

import snakemake

from ymp.common import AttrDict, ensure_list
from ymp.exceptions import YmpException
from ymp.snakemake import BaseExpander, get_workflow, WorkflowObject


log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class YmpEnvError(YmpException):
    """Failure in env"""


class Env(WorkflowObject, snakemake.conda.Env):
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

    @staticmethod
    def get_installed_env_hashes():
        from ymp.config import icfg
        return [
            dentry.name
            for dentry in os.scandir(icfg.absdir.conda_prefix)
            if dentry.is_dir()
        ]

    def __init__(self, env_file: Optional[str] = None,
                 name: Optional[str] = None,
                 packages: Optional[Union[list, str]] = None,
                 base: str = "none",
                 channels: Optional[Union[list, str]] = None) -> None:
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
        from ymp.config import icfg

        # must have either name or env_file:
        if (name and env_file) or not (name or env_file):
            raise YmpEnvError("Env must have exactly one of `name` and `file`")

        if name:
            self.name = name
        else:
            self.name, _ = op.splitext(op.basename(env_file))

        if env_file:
            self.dynamic = False
            self.filename = env_file
            self.lineno = 1
        else:
            self.dynamic = True

            env_file = op.join(icfg.absdir.dynamic_envs, f"{name}.yml")
            with icfg.conda.defaults[base] as defaults:
                defaults.dependencies.extend(ensure_list(packages))
                defaults.channels.extend(ensure_list(channels))
                contents = f"name: {name}\n{defaults}"
            disk_contents = ""
            if op.exists(env_file):
                with open(env_file, "r") as inf:
                    disk_contents = inf.read()
            if contents != disk_contents:
                with open(env_file, "w") as out:
                    out.write(contents)

        pseudo_dag = AttrDict({
            'workflow': {
                'persistence': {
                    'conda_env_path': icfg.absdir.conda_prefix,
                    'conda_env_archive_path': icfg.absdir.conda_archive_prefix
                }
            }
        })

        super().__init__(env_file, pseudo_dag)

    def create(self):
        """Create conda environment""

        Inherits from snakemake.conda.Env.create
        """
        log.warning("Creating environment '%s'", self.name)
        log.warning("Target dir is '%s'", self.path)
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

    def export(self, dest, create=True, overwrite=False):
        """Freeze environment"""
        if os.path.exists(dest) and not overwrite:
            raise YmpEnvError(
                f"Cannot export environment '{self.name}' to '{dest}': "
                f"file exists")
        if not self.installed:
            if create:
                self.create()
            else:
                raise YmpEnvError(
                    f"Cannot export environment '{self.name}': "
                    f"not installed")
        log.warning("Exporting environment '%s' to '%s'", self.name, dest)
        res = subprocess.run([
            "conda", "env", "export",
            "-p", self.path,
            "-f", dest
        ])
        if res.returncode != 0:
            raise YmpEnvError(f"Failed to export environment {self.name}")

    def __lt__(self, other):
        "Comparator for sorting"
        return self.name < other.name

    def __repr__(self):
        return f"{self.__class__.__name__}({self.__dict__!r})"


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

        self._search_paths = config.conda.env_path
        self._envs = None

    def expands_field(self, field):
        return field == 'conda_env'

    def format(self, conda_env, *args, **kwargs):
        if not self._envs:
            self._envs = Env.get_registry()
        if conda_env in self._envs:
            return self._envs[conda_env].file

        for snakefile in reversed(self._workflow.included_stack):
            basepath = op.dirname(snakefile)
            for _, relpath in sorted(self._search_paths.items()):
                searchpath = op.join(basepath, relpath)
                abspath = op.abspath(op.join(searchpath, conda_env))
                for ext in "", ".yml", ".yaml":
                    env_file = abspath+ext
                    if op.exists(env_file):
                        Env(env_file)
                        return env_file
        return conda_env
