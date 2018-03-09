from glob import glob
import logging
import os.path as op
from pkg_resources import resource_filename
import subprocess
import snakemake

from ymp.snakemake import BaseExpander


log = logging.getLogger(__name__)


class CondaPathExpander(BaseExpander):
    """Applies search path for conda environment specifications

    File names supplied via `rule: conda: "some.yml"` are replaced with
    absolute paths if they are found in any searched directory.
    Each `search_paths` entry is appended to the directory
    containing the top level Snakefile and the directory checked for
    the filename. Thereafter, the stack of including Snakefiles is traversed
    backwards. If no file is found, the original name is returned.
    """
    def __init__(self, search_paths, *args, **kwargs):
        try:
            from snakemake.workflow import workflow
            self._workflow = workflow
            super().__init__(*args, **kwargs)
        except:
            log.debug("CondaPathExpander not registered -- needs snakemake")

        self._search_paths = search_paths

    def expands_field(self, field):
        return field == 'conda_env'

    def format(self, conda_env, *args, **kwargs):
        for snakefile in reversed(self._workflow.included_stack):
            basepath = op.dirname(snakefile)
            for _, relpath in sorted(self._search_paths.items()):
                searchpath = op.join(basepath, relpath)
                abspath = op.abspath(op.join(searchpath, conda_env))
                if op.exists(abspath):
                    return abspath
        return conda_env


class Env(snakemake.conda.Env):
    "Represents YMP conda environment"

    _env_dir = op.expanduser("~/.ymp/conda")
    _env_archive_dir = op.expanduser("~/.ymp/conda_archive")

    def __init__(self, env_file):
        self.file = env_file
        self.name, _ = op.splitext(op.basename(env_file))

        self._hash = None
        self._content_hash = None
        self._content = None
        self._path = None
        self._archive_file = None

    def create(self):
        "Create conda environment"
        log.warning("Creating environment '%s'", self.name)
        return super().create()

    def update(self):
        "Update conda environment"
        # call create to make sure environment exists
        self.create()
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
