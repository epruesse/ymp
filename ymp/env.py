"""
This module manages the conda environments.
"""

from glob import glob
import logging
import os.path as op
from pkg_resources import resource_filename
import subprocess
import snakemake

from ymp.snakemake import BaseExpander
from ymp.common import ensure_list


log = logging.getLogger(__name__)


class MetaEnv(type):
    _CFG = None
    _ICFG = None
    _ENVS = {}

    @property
    def icfg(cls):
        if not cls._ICFG:
            from ymp.config import icfg
            cls._ICFG = icfg
        return cls._ICFG

    @property
    def cfg(cls):
        if not cls._CFG:
            cls._CFG = cls.icfg.conda
            for path in ('conda_prefix', 'conda_archive_prefix'):
                cls._CFG[path] = op.expanduser(cls._CFG[path])
                # import pdb; pdb.set_trace()
            cls._CFG.env_path[50] = cls.icfg.absdir.dynamic_envs
        return cls._CFG

    def new(cls, name, packages, base="none", channels=[]):
        with cls.cfg.defaults[base] as defaults:
            defaults.dependencies.extend(ensure_list(packages))
            defaults.channels.extend(ensure_list(channels))
            contents = f"name: {name}\n{defaults}"
            fname = op.join(cls.icfg.absdir.dynamic_envs,
                            f"{name}.yml")
            with open(fname, "w") as f:
                f.write(contents)
                cls._ENVS[name] = fname


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

    _CFG = None

    def __init__(self, env_file):
        # We initialize ourselves, rather than referring to super(),
        # because we lack a snakemake persistance dag object required
        # by snakemake.conda.Env to initialize _env_dir and _env_archive_dir

        self.file = env_file
        self.name, _ = op.splitext(op.basename(env_file))
        self._env_dir = self.cfg.conda_prefix
        self._env_archive_dir = self.cfg.conda_archive_prefix
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

    def update(self):
        "Update conda environment"
        self.create()  # call create to make sure environment exists
        log.warning("Updating environment '%s'", self.name)
        return subprocess.run([
            "conda",  "env", "update",
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
        except:
            log.debug("CondaPathExpander not registered -- needs snakemake")

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


if False:
    by_name = {
        env.name: env for env in (
            Env(fname) for fname in glob(
                resource_filename("ymp", "rules/*.yml")
            )
        )
    }

    by_hash = {
        env.hash: env for env in by_name.values()
    }

    by_path = {
        env.path: env for env in by_name.values()
    }

    dead = {
        op.basename(path): path
        for path in glob(Env._env_dir + "/*")
        if path not in by_path
    }
