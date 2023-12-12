"""
This module manages the conda environments.
"""

import io
import logging
import os
import os.path as op
import shutil
import subprocess
from typing import Optional, Union

from ruamel.yaml import YAML

import snakemake.deployment.conda as snakemake_conda

from snakemake.rules import Rule

import ymp
from ymp.common import AttrDict, ensure_list
from ymp.exceptions import YmpRuleError, YmpWorkflowError
from ymp.snakemake import BaseExpander, WorkflowObject


log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class Env(WorkflowObject, snakemake_conda.Env):
    """Represents YMP conda environment

    Snakemake expects the conda environments in a per-workflow
    directory configured by ``conda_prefix``. YMP sets this value by
    default to ``~/.ymp/conda``, which has a greater chance of being
    on the same file system as the conda cache, allowing for hard
    linking of environment files.

    Within the folder ``conda_prefix``, each environment is created in
    a folder named by the hash of the environment definition file's
    contents and the ``conda_prefix`` path. This class inherits from
    ``snakemake.deployment.conda.Env`` to ensure that the hash we use
    is identical to the one Snakemake will use during workflow
    execution.

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

    def __new__(cls, *args, **kwargs):
        if args and "name" not in kwargs:
            for env in cls.get_registry().values():
                if env.file == args[0]:
                    return env
        return super().__new__(cls)

    def __init__(
            self,
            # Snakemake Params:
            workflow = None,
            env_file: Optional[str] = None,
            env_name: Optional[str] = None,
            env_dir = None,
            container_img=None,
            cleanup=None,
            # YMP Params:
            name: Optional[str] = None,
            packages: Optional[Union[list, str]] = None,
            base: str = "none",
            channels: Optional[Union[list, str]] = None
    ) -> None:
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
        if 'name' in self.__dict__:
            # already initialized
            return
        cfg = ymp.get_config()

        if env_file:
            if name:
                raise YmpRuleError(
                    self,
                    "Env must not have both 'name' and 'env_file' parameters'"
                )
            self.dynamic = False
            self._ymp_name, _ = op.splitext(op.basename(env_file))
            self.packages = None
            self.base = None
            self.channels = None

            # Override location for exceptions:
            self.filename = env_file
            self.lineno = 1
        elif name:
            self.dynamic = True
            self._ymp_name = name
            self.packages = ensure_list(packages) + cfg.conda.defaults[base].dependencies
            self.channels = ensure_list(channels) + cfg.conda.defaults[base].channels
            env_file = op.join(cfg.ensuredir.dynamic_envs, f"{name}.yml")
            contents = self._get_dynamic_contents()
            self._update_file(env_file, contents)
        else:
            raise YmpRuleError(
                self,
                "Env must have either 'name' or 'env_file' parameter"
            )

        # Unlike within snakemake, we create these objects before the workflow is fully
        # initialized, which means we need to create a fake one:
        if not workflow:
            workflow = AttrDict({
                'persistence': {
                    'conda_env_path': cfg.ensuredir.conda_prefix,
                    'conda_env_archive_path': cfg.ensuredir.conda_archive_prefix,
                },
                'conda_frontend': cfg.conda.frontend,
                'singularity_args': '',
            })

        super().__init__(
            workflow = workflow,
            env_file = env_file,
            env_dir = env_dir if env_dir else cfg.ensuredir.conda_prefix,
            container_img = container_img,
            cleanup = cleanup
        )

        self.register()

    def _get_dynamic_contents(self):
        cfg = ymp.get_config()
        defaults = {
            'name': self._ymp_name,
            'dependencies': self.packages,
            'channels': self.channels,
        }
        yaml = YAML(typ='rt')
        yaml.default_flow_style = False
        buf = io.StringIO()
        yaml.dump(defaults, buf)
        return buf.getvalue()

    @staticmethod
    def _update_file(env_file, contents):
        disk_contents = ""
        if op.exists(env_file):
            with open(env_file, "r") as inf:
                disk_contents = inf.read()
        if contents != disk_contents:
            with open(env_file, "w") as out:
                out.write(contents)

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['workflow']
        return state

    def __setstate(self, state):
        self.__dict__.update(state)
        self.workflow = ymp.get_config().workflow

    @property
    def _env_archive_dir(self):
        cfg = ymp.get_config()
        return cfg.ensuredir.conda_archive_prefix

    def _get_content(self):
        if self.dynamic:
            return self._get_dynamic_contents().encode("utf-8")
        cfg = ymp.get_config()
        if cfg.workflow:
            self.workflow = cfg.workflow
            return super()._get_content()
        return open(self.file, "rb").read()


    def set_prefix(self, prefix):
        self._env_dir = op.abspath(prefix)

    def create(self, dryrun=False, reinstall=None, nospec=None, noarchive=None):
        """Ensure the conda environment has been created

        Inherits from snakemake.deployment.conda.Env.create

        Behavior of super class
            - Resolve remote file
            - If containerized, check environment path exists and return if true
            - Check for interrupted env create, delete if so
            - Return if environment exists
            - Install from archive if env_archive exists
            - Install using self.frontent if not_careful

        Handling pre-computed environment specs
            In addition to freezing environments by maintaining a copy of
            the package binaries, we allow maintaining a copy of the
            package binary URLs, from which the archive folder is populated
            on demand. We just download those to self.archive and pass on.

        Parameters:
          - reinstall: force re-installing already installed envs
          - noarchive: delete existing archives before installing, forcing re-download
          - nospec: do not use stored spec ("lock", set of urls for env)
        """
        cfg = ymp.get_config()
        if nospec is None:
            nospec = cfg.conda.create.nospec
        if noarchive is None:
            noarchive = cfg.conda.create.noarchive
        if reinstall is None:
            reinstall = cfg.conda.create.reinstall

        if self.installed:
            if reinstall:
                log.info("Environment '%s' already exists. Removing...", self._ymp_name)
                if not dryrun:
                    shutil.rmtree(self.address, ignore_errors = True)
            else:
                log.info("Environment '%s' already exists", self._ymp_name)
                return self.address

        log.warning("Creating environment '%s'", self._ymp_name)
        log.debug("Target dir is '%s'", self.address)

        if noarchive and self.archive_file:
            log.warning("Removing archived environment packages...")
            if not dryrun:
                shutil.rmtree(self.archive_file, ignore_errors = True)

        if not self._have_archive() and not nospec:
            urls, files, md5s = self._get_env_from_spec()
            if files:
                if dryrun:
                    log.info("Would download %i files", len(urls))
                else:
                    self._download_files(urls, md5s)
                    packages = op.join(self.archive_file, "packages.txt")
                    with open(packages, "w") as f:
                        f.write("\n".join(files) + "\n")
            else:
                log.warning("Neither spec file nor package archive found for '%s',"
                            " falling back to native resolver", self._ymp_name)

        res = super().create(dryrun)
        log.info("Created env %s", self._ymp_name)
        return res

    def _have_archive(self):
        packages_txt = op.join(self.archive_file, "packages.txt")
        log.info("Checking for archive in %s", self.archive_file)
        if not op.exists(packages_txt):
            return False
        log.info("... found. Checking archive.")
        with open(packages_txt) as f:
            packages = [package.strip() for package in f]
        missing_packages = [
            package for package in packages
            if not op.exists(op.join(self.archive_file, package))
        ]
        if missing_packages:
            log.warning(
                "Ignoring incomplete package archive for environment %s",
                self._ymp_name)
            log.debug(
                "Missing packages: %s", missing_packages)
            return False
        return True

    def _get_env_from_spec(self):
        """Parses conda spec file

        Conda spec files contain a list of URLs pointing to the packages
        comprising the environment. Each URL may have an md5 sum appended
        as "anchor" using "#". Comments are placed at the top in lines
        beginnig with "#" and a single line "@EXPLICIT" indicates the type
        of the file.

        Returns:
          urls: list of URLs
          files: list of file names
          md5s: list of md5 sums
        """
        cfg = ymp.get_config()
        for spec_path in cfg.conda.env_specs.get_paths():
            if spec_path.startswith("BUILTIN:"):
                spec_path = spec_path.replace("BUILTIN:", "")
                spec_path = op.join(ymp._env_dir, spec_path)
            for path in (op.join(spec_path, cfg.platform), spec_path):
                spec_file = op.join(path, self._ymp_name + ".txt")
                log.debug("Trying %s", spec_file)
                if op.exists(spec_file):
                    log.info("Using %s", spec_file)
                    break
            else:
                continue
            break
        else:
            return [], [], []

        log.debug("Using env spec '%s'", spec_file)

        with open(spec_file) as sf:
            urls = [line for line in sf
                    if line and line[0] != "@" and line[0] != "#"]
        md5s = [url.split("#")[1] for url in urls]
        files = [url.split("#")[0].split("/")[-1] for url in urls]

        return urls, files, md5s

    def _download_files(self, urls, md5s):
        from ymp.download import FileDownloader
        if not op.exists(self.archive_file):
            os.makedirs(self.archive_file)
        cfg = ymp.get_config()
        fd = FileDownloader(alturls=cfg.conda.alturls)
        if not fd.get(urls, self.archive_file, md5s):
            # remove partially download archive folder?
            # shutil.rmtree(self.archive_file, ignore_errors=True)
            raise YmpWorkflowError(
                f"Unable to create environment {self._ymp_name}, "
                f"because downloads failed. See log for details.")

    @property
    def label(self):
        return self._ymp_name

    @property
    def installed(self):
        if self.is_containerized:
            return True  # Not checking
        if not op.exists(self.address):
            return False
        start_stamp = op.join(self.address, "env_setup_start")
        finish_stamp = op.join(self.address, "env_setup_done")
        if op.exists(start_stamp) and not op.exists(finish_stamp):
            return False
        return True

    def update(self):
        "Update conda environment"
        self.create()  # call create to make sure environment exists
        log.warning("Updating environment '%s'", self._ymp_name)
        log.warning(f"Running {self.frontend} env update --prune -p {self.address} -f {self.file} -v")
        return subprocess.run([
            self.frontend, "env", "update",
            "--prune",
            "-p", str(self.address),
            "-f", str(self.file),
            "-v"
        ]).returncode

    def run(self, command):
        """Execute command in environment

        Returns exit code of command run.
        """
        command = " ".join(command)
        command = snakemake_conda.Conda().shellcmd(self.address, command)
        cfg = ymp.get_config()
        log.debug("Running: %s", command)
        return subprocess.run(
            command,
            shell=True,
            executable=cfg.shell
        ).returncode

    def export(self, stream, typ='yml'):
        """Freeze environment"""
        log.warning("Exporting environment '%s'", self._ymp_name)
        if typ == 'yml':
            res = subprocess.run([
                "conda", "env", "export",
                "-p", self.address,
            ], stdout=subprocess.PIPE)

            yaml = YAML(typ='rt')
            yaml.default_flow_style = False
            env = yaml.load(res.stdout)
            env['name'] = self._ymp_name
            if 'prefix' in env:
                del env['prefix']
            yaml.dump(env, stream)
        elif typ == 'txt':
            res = subprocess.run([
                "conda", "list", "--explicit", "--md5",
                "-p", self.address,
            ], stdout=stream)
        return res.returncode

    def __lt__(self, other):
        "Comparator for sorting"
        return self._ymp_name < other._ymp_name

    def __repr__(self):
        return f"{self.__class__.__name__}({self.__dict__!r})"

    def __eq__(self, other):
        if isinstance(other, Env):
            return self.hash == other.hash


# Patch Snakemake's Env class with our own
snakemake_conda.Env = Env


class CondaPathExpander(BaseExpander):
    """Applies search path for conda environment specifications

    File names supplied via ``rule: conda: "some.yml"`` are replaced with
    absolute paths if they are found in any searched directory.
    Each ``search_paths`` entry is appended to the directory
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
            return self._envs[conda_env].file.get_path_or_uri()

        for snakefile in reversed(self.workflow.included_stack):
            basepath = op.dirname(snakefile.get_path_or_uri())
            for _, relpath in sorted(self._search_paths.items()):
                searchpath = op.join(basepath, relpath)
                abspath = op.abspath(op.join(searchpath, conda_env))
                for ext in "", ".yml", ".yaml":
                    env_file = abspath+ext
                    if op.exists(env_file):
                        Env(env_file = env_file)
                        return env_file
        return conda_env
