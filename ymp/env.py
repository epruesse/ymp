from glob import glob
import logging
from os.path import basename, expanduser, splitext
from pkg_resources import resource_filename
import subprocess

import snakemake

log = logging.getLogger(__name__)


class Env(snakemake.conda.Env):
    "Represents YMP conda environment"

    _env_dir = expanduser("~/.ymp/conda")
    _env_archive_dir = expanduser("~/.ymp/conda_archive")

    def __init__(self, env_file):
        self.file = env_file
        self.name, _ = splitext(basename(env_file))

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
            "-f", self.file
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
    basename(path): path
    for path in glob(Env._env_dir + "/*")
    if path not in by_path
}
