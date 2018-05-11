"""
This module manages the conda environments.
"""

import io
import logging
import os
import os.path as op
import shutil
import subprocess
from glob import glob
from typing import Optional, Union

from ruamel.yaml import YAML

import snakemake
import snakemake.conda
from snakemake.rules import Rule

import ymp
from ymp.common import AttrDict, ensure_list
from ymp.exceptions import YmpRuleError, YmpWorkflowError
from ymp.snakemake import BaseExpander, WorkflowObject


log = logging.getLogger(__name__)  # pylint: disable=invalid-name


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
        cfg = ymp.get_config()
        return [
            dentry.name
            for dentry in os.scandir(cfg.absdir.conda_prefix)
            if dentry.is_dir()
        ]

    def __init__(self, env_file: Optional[str] = None,
                 dag: Optional[object] = None,
                 singularity_img=None,
                 name: Optional[str] = None,
                 packages: Optional[Union[list, str]] = None,
                 base: str = "none",
                 channels: Optional[Union[list, str]] = None,
                 rule: Optional[Rule] = None) -> None:
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
        cfg = ymp.get_config()

        pseudo_dag = AttrDict({
            'workflow': {
                'persistence': {
                    'conda_env_path': cfg.absdir.conda_prefix,
                    'conda_env_archive_path': cfg.absdir.conda_archive_prefix
                }
            }
        })

        # must have either name or env_file:
        if (name and env_file) or not (name or env_file):
            raise YmpRuleError(
                self,
                "Env must have exactly one of `name` and `file`")

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

            env_file = op.join(cfg.ensuredir.dynamic_envs, f"{name}.yml")
            defaults = {
                'name': self.name,
                'dependencies': list(ensure_list(packages) +
                                     cfg.conda.defaults[base].dependencies),
                'channels': list(ensure_list(channels) +
                                 cfg.conda.defaults[base].channels)
            }
            yaml = YAML(typ='rt')
            yaml.default_flow_style = False
            buf = io.StringIO()
            yaml.dump(defaults, buf)
            contents = buf.getvalue()

            disk_contents = ""
            if op.exists(env_file):
                with open(env_file, "r") as inf:
                    disk_contents = inf.read()
            if contents != disk_contents:
                with open(env_file, "w") as out:
                    out.write(contents)

        super().__init__(env_file, pseudo_dag, singularity_img)
        self.register()

    def set_prefix(self, prefix):
        self._env_dir = os.path.abspath(prefix)

    def create(self, dryrun=False):
        """Ensure the conda environment has been created""

        Inherits from snakemake.conda.Env.create

        Behavior of super class
        ~~~~~~~~~~~~~~~~~~~~~~~

        The environment is installed in a folder in ``conda_prefix``
        named according to a hash of the ``environment.yaml`` defining
        the environment and the value of ``conda-prefix``
        (``Env.hash``). The latter is included as installed
        environments cannot be moved.

        - If this folder (``Env.path``) exists, nothing is done.

        - If a folder named according to the hash of just the contents
        of ``environment.yaml`` exists, the environment is created by
        unpacking the tar balls in that folder.

        Handling pre-computed environment specs
        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        In addition to freezing environments by maintaining a copy of
        the package binaries, we allow maintaining a copy of the
        package binary URLs, from which the archive folder is populated
        on demand.

        If a file ``{Env.name}.txt`` exists in ``conda.spec

        """
        cfg = ymp.get_config()

        # Skip if environment already exists
        if os.path.exists(self.path):
            log.info("Environment '%s' already exists", self.name)
            return self.path
        log.warning("Creating environment '%s'", self.name)
        log.debug("Target dir is '%s'", self.path)

        files = []
        urls = []
        install_files = []

        # Try to get urls, md5s and files from env spec
        if cfg.conda.env_specs:
            spec_file = os.path.join(
                ymp._env_dir,
                cfg.conda.env_specs,
                cfg.platform,
                self.name + ".txt"
            )
            if os.path.exists(spec_file):
                with open(spec_file) as sf:
                    urls = [line for line in sf
                            if line and line[0] != "@" and line[0] != "#"]

                md5s = [url.split("#")[1] for url in urls]
                files = [url.split("#")[0].split("/")[-1] for url in urls]
                log.debug("Using env spec '%s'", spec_file)

        if os.path.exists(self.archive_file):
            found_files = glob(os.path.join(self.archive_file, "*.tar.bz2"))
            if files:
                # Sort files according to spec file as far as possible
                files = ([fn for fn in files if fn in found_files]
                         + [fn for fn in found_files if fn not in files])
            else:
                files = found_files
            install_files = [os.path.join(self.archive_file, fn)
                             for fn in files]
        elif urls:
            from ymp.common import FileDownloader
            if dryrun:
                log.info("Would download %i files", len(urls))
            else:
                dest = self.archive_file
                os.makedirs(dest)
                res = FileDownloader().get(urls, dest, md5s)
                if not res:
                    # remove partially download archive folder
                    shutil.rmtree(self.archive_file, ignore_errors=True)
                    raise YmpWorkflowError(
                        f"Unable to create environment {self.name}, "
                        f"because downloads failed. See log for details.")
                install_files = [os.path.join(dest, fn) for fn in files]

        # Install from packages if we have packages
        # We re-implement this here although superclass does it, because
        # 1. superclass will passes `--copy` to conda, forcing it to bypass
        #    hard linking option, wasting time and space
        # 2. superclass will not order files correctly
        # This is also done by Snakemake's Env, reproduced here
        # only because Snakemake passed "--copy" to conda, forcing it
        # to copy rather than hard link files.
        if install_files:
            log.info("Installing environment '%s' from %i package files",
                     self.name, len(install_files))
            log.debug("Files: %s", install_files)
            if not dryrun:
                log.info("Calling conda...")
                sp = subprocess.run(["conda", "create", "--prefix",
                                     self.path] + install_files)
                if sp.returncode != 0:
                    # make sure we don't leave partially installed env around
                    shutil.rmtree(self.path, ignore_errors=True)
                    raise YmpWorkflowError(
                        f"Unable to create environment {self.name}, "
                        f"because conda create failed"
                    )
                log.info("Conda complete")
        else:
            log.warning("Neither spec file nor package archive found for '%s',"
                        " falling back to native resolver", self.name)

        res = super().create(dryrun)
        log.info("Created env %s", self.name)
        return res

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

    def export(self, stream, typ='yml'):
        """Freeze environment"""
        log.warning("Exporting environment '%s'", self.name)
        if typ == 'yml':
            res = subprocess.run([
                "conda", "env", "export",
                "-p", self.path,
            ], stdout=subprocess.PIPE)

            yaml = YAML(typ='rt')
            yaml.default_flow_style = False
            env = yaml.load(res.stdout)
            env['name'] = self.name
            del env['prefix']
            yaml.dump(env, stream)
        elif typ == 'txt':
            res = subprocess.run([
                "conda", "list", "--explicit", "--md5",
                "-p", self.path,
            ], stdout=stream)
        return res.returncode

    def __lt__(self, other):
        "Comparator for sorting"
        return self.name < other.name

    def __repr__(self):
        return f"{self.__class__.__name__}({self.__dict__!r})"

    def __eq__(self, other):
        if isinstance(other, Env):
            return self.hash == other.hash


# Patch Snakemake's Env class with our own
snakemake.conda.Env = Env


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
        super().__init__(*args, **kwargs)
        self._search_paths = config.conda.env_path
        self._envs = None

    def expands_field(self, field):
        return field == 'conda_env'

    def format(self, conda_env, *args, **kwargs):
        if conda_env[0] == "/" and op.exists(conda_env):
            return conda_env
        if not self._envs:
            self._envs = Env.get_registry()
        if conda_env in self._envs:
            return self._envs[conda_env].file

        for snakefile in reversed(self.workflow.included_stack):
            basepath = op.dirname(snakefile)
            for _, relpath in sorted(self._search_paths.items()):
                searchpath = op.join(basepath, relpath)
                abspath = op.abspath(op.join(searchpath, conda_env))
                for ext in "", ".yml", ".yaml":
                    env_file = abspath+ext
                    if op.exists(env_file):
                        Env(env_file, rule=self.current_rule)
                        return env_file
        return conda_env
