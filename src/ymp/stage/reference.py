import logging
import os
import re
from hashlib import sha1
from typing import Dict, Optional, Union, Set, List
from collections.abc import Mapping, Sequence

from snakemake.rules import Rule

from ymp.snakemake import make_rule
from ymp.util import make_local_path
from ymp.stage import ConfigStage, Activateable, Stage
from ymp.exceptions import YmpConfigError


log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class Archive(object):
    name = None
    hash = None
    tar = None
    dirname = None
    strip_components = None
    files = None

    def __init__(self, name, dirname, tar, strip, files):
        self.name = name
        self.dirname = dirname
        self.tar = tar
        self.strip = strip
        self.files = files

        self.hash = sha1(self.tar.encode("utf-8")).hexdigest()[:8]
        self.prefix = os.path.join(self.dirname, "_unpacked_" + self.hash)

    def get_files(self):
        if isinstance(self.files, Sequence):
            return {fn: os.path.join(self.prefix, fn) for fn in self.files}
        elif isinstance(self.files, Mapping):
            return {
                fn_ymp: os.path.join(self.prefix, fn_arch)
                for fn_ymp, fn_arch in self.files.items()
            }
        else:
            raise Exception("unknown data type for reference.files")

    def make_unpack_rule(self, baserule: "Rule"):
        docstr_tpl = """
        Unpacks {} archive:

        Files:
        """

        item_tpl = """
        - {}
        """
        docstr = "\n".join(
            [docstr_tpl.format(self.name)] + [item_tpl.format(fn) for fn in self.files]
        )
        return make_rule(
            name="unpack_{}_{}".format(self.name, self.hash),
            docstring=docstr,
            lineno=0,
            snakefile=__name__,
            parent=baserule,
            input=([], {"tar": self.tar}),
            output=([], {"files": list(self.get_files().values())}),
            params=([], {"strip": self.strip, "prefix": self.prefix}),
        )


class Resource:
    """References comprise files, possibly remote, spefied as
    "resources". These could e.g. be a archive (tar.gz), a local
    directory or individual files. This is the base class for resource
    types that can be configured.

    """

    _registry = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)  # recurse up if subsubclass
        if not getattr(cls, "type_names", []):  # no type no registering
            return
        for name in cls.type_names:
            if name in Resource._registry:
                raise ValueError(
                    f"Resource class '{cls.__name__}' defines duplicate type name '{name}'"
                    f" already registered for "
                    f"'{Resource._registry[name].__name__}'."
                )
        Resource._registry.update({name: cls for name in cls.type_names})

    def __init__(self, ref, cfg):
        self.reference = ref
        self.cfg = cfg
        self.type_name = self.get_type_name(cfg)
        self._ids: Set[str] = set()
        self.id_name = self.get_id_name(cfg)

    @classmethod
    def make_from_cfg(cls, ref, cfg, num):
        rsc = cfg[num]
        if rsc is None:
            raise YmpConfigError(cfg, "Empty reference resource config?!", key=num)
        if not isinstance(rsc, Mapping):
            raise YmpConfigError(
                cfg, "Reference resource config must be a key-value mapping", key=num
            )
        type_name = Resource.get_type_name(rsc)
        klass = Resource._registry.get(type_name)
        if klass is None:
            raise YmpConfigError(rsc, f"Unknown type {type_name}", key="type")
        return klass(ref, rsc)

    @staticmethod
    def get_type_name(rsc):
        return rsc.get("type", "fasta").lower()

    def get_local_path(self, rsc, field="url"):
        """Extract local file path from config field

        - File paths for remote URLs are rewritten to point configured
          downloads folder so that download is run by download rule.
        - Relative paths are interpreted relative to the config file
          defining the url, unless ``!workdir`` is prefixed, in which
          case it's relative to the main ymp.yml.

        """
        if not "url" in rsc:
            raise YmpConfigError(
                rsc,
                f"Reference resource of type '{self.type_name}' must have '{field}' field",
            )
        import ymp

        cfg = ymp.get_config()
        local_path = make_local_path(cfg, str(rsc[field]))
        if not local_path != rsc[field]:
            # unchanged by download redirect, honor the relative path:
            local_path = rsc.get_path(field)
        return local_path

    def get_id_name(self, rsc):
        id = "ALL"
        if "id" in rsc:
            id = rsc["id"]
            self._ids.add(id)
        return id

    def generate_rules(self, **kwargs_):
        """Generate special rules needed for the resource type"""
        yield None


class UrlResource(Resource):
    def __init__(self, *args):
        super().__init__(*args)
        self.local_path = self.get_local_path(self.cfg)


class FileResource(UrlResource):
    type_names = ["file", "direct"]

    def __init__(self, *args):
        super().__init__(*args)
        self.extension = self.get_extension(self.cfg)
        self.files = {f"{self.id_name}.{self.extension}": self.local_path}

    def get_extension(self, cfg):
        ext = cfg.get("extension")
        if not ext:
            raise YmpConfigError(
                cfg, "Reference resource of type direct must have 'extension' field"
            )
        return ext


class NamedResource(FileResource):
    type_names = ["fasta", "fastp", "tx.fasta"]

    def get_extension(self, cfg):
        return self.type_name + ".gz"


class NamedUnpackedResources(FileResource):
    type_names = ["gtf", "snp", "tsv", "csv"]

    def get_extension(self, cfg):
        return self.type_name


class ArchiveResource(UrlResource):
    type_names = ["archive", "dir"]

    def __init__(self, *args):
        super().__init__(*args)
        if not "files" in self.cfg:
            raise YmpConfigError(
                self.cfg, "Reference resource of type archive must have 'files' field"
            )
        files = self.cfg.get("files")
        if (
            not isinstance(files, Mapping)
            and not isinstance(files, Sequence)
            or isinstance(files, str)
        ):
            raise YmpConfigError(
                self.cfg, "Archive 'files' must be mapping", key="files"
            )
        self.archive = Archive(
            name="NAME",
            dirname=self.reference.canonical_location(),
            tar=self.local_path,
            files=files,
            strip=self.cfg.get("strip_components", 0),
        )
        self.files = self.archive.get_files()

    def generate_rules(self, **kwargs):
        yield self.archive.make_unpack_rule(kwargs["unpack_archive"])


class LocalDirResource(UrlResource):
    type_names = ["localdir", "dirx"]

    def __init__(self, *args):
        super().__init__(*args)
        if not "files" in self.cfg:
            raise YmpConfigError(
                self.cfg, "Reference resource of type localdir must have 'files' field"
            )
        files = self.cfg.get("files")
        if not isinstance(files, Mapping):
            raise YmpConfigError(
                self.cfg, "Localdir 'files' must be mapping", key="files"
            )

        self.files = {
            key: os.path.join(self.local_path, val) for key, val in files.items()
        }


class RegexLocalDirResource(UrlResource):
    type_names = ["path"]

    def __init__(self, *args):
        super().__init__(*args)
        if not "match" in self.cfg:
            raise YmpConfigError(
                self.cfg, "Reference resource of type path must have 'match' field"
            )
        matchlist = self.cfg.get("match")
        if not isinstance(matchlist, Sequence) or isinstance(matchlist, str):
            raise YmpConfigError(self.cfg, "Path 'match' must be list", key="match")

        try:
            filenames = os.listdir(self.local_path)
        except FileNotFoundError:
            raise YmpConfigError(
                self.cfg, "Directory required by path resource inaccessible"
            )
        self.dir = self.local_path.rstrip("/")

        self.files = {}
        for num, regex in enumerate(matchlist):
            try:
                comp_regex = re.compile(regex)
            except re.error as exc:
                raise YmpConfigError(
                    matchlist, f"Regex failed to compile: {exc}", key=num
                ) from exc

            if list(comp_regex.groupindex) != ["sample"]:
                raise YmpConfigError(
                    matchlist,
                    "Path resource match regexp's must have exactly one "
                    "named wildcard called 'sample'",
                    key=num,
                )

            for filename in filenames:
                match = comp_regex.fullmatch(filename)
                if match:
                    self._ids.add(match.group("sample"))
                    self.files[filename] = os.path.join(self.local_path, filename)

        if not self.files:
            raise YmpConfigError(
                self.cfg, "Reference resource of type path found no files!"
            )


class Reference(Activateable, ConfigStage):
    """
    Represents (remote) reference file/database configuration
    """

    def __init__(self, name, cfg):
        super().__init__("ref_" + name, cfg)
        #: Files provided by the reference. Keys are the file names
        #: within ymp ("target.extension"), symlinked into dir.ref/ref_name/ and
        #: values are the path to the reference file from workspace root.
        self.files: Dict[str, str] = {}
        #: Name without the ref_ prefix
        self.plainname = name
        self.archives = []
        self._outputs = None
        self.cfg = cfg

        self.dir = self.canonical_location()

        if not isinstance(cfg, Sequence) or isinstance(cfg, str):
            raise YmpConfigError(cfg, "Reference config must be list")

        self._resources = [
            Resource.make_from_cfg(self, cfg, num) for num in range(len(cfg))
        ]

        self._ids: Set[str] = set.union(*(rsc._ids for rsc in self._resources))
        self._files: Dict[str, str] = {}
        for rsc in self._resources:
            for name, path in rsc.files.items():
                if name in self._files:
                    raise YmpConfigError(rsc.cfg, "Duplicate File")
                self._files[name] = path

        # Copy rules defined in primary references stage
        stage_references = Stage.get_registry().get("references")
        if not stage_references:
            raise YmpConfigError(
                cfg, "Reference base stage not found. Main rules not loaded?"
            )
        self.rules = stage_references.rules.copy()

    def canonical_location(self):
        import ymp

        cfg = ymp.get_config()
        basedir = cfg.dir.references
        return os.path.join(basedir, self.plainname)

    def get_group(self, stack: "StageStack", default_groups: List[str]) -> List[str]:
        if len(self._ids) > 1:
            groups = [self.name]
        else:
            groups = []
        return super().get_group(stack, groups)

    def get_ids(
        self,
        stack: "StageStack",
        groups: List[str],
        match_groups: Optional[List[str]] = None,
        match_value: Optional[str] = None,
    ) -> List[str]:
        if self._ids:
            return list(self._ids)
        return super().get_ids(stack, groups, match_groups, match_value)

    @property
    def outputs(self) -> Union[Set[str], Dict[str, str]]:
        if self._outputs is None:
            keys = self._ids if self._ids else ["ALL"]
            self._outputs = {
                "/"
                + re.sub(f"(^|.)({'|'.join(keys)})\.", r"\1{sample}.", fname): "."
                + self.name
                for fname in self._files
            }
        return self._outputs

    def get_path(self, _stack):
        return self.dir

    def get_all_targets(self, stack: "StageStack") -> List[str]:
        return [os.path.join(self.dir, fname) for fname in self._files]

    def get_file(self, filename, isdir=False):
        local_path = self._files.get(filename)
        if local_path:
            if os.path.isdir(local_path) != isdir:
                return "YMP_THIS_FILE_MUST_NOT_EXIST"
            return local_path
        log.error(f"{self!r}: Failed to find {filename}")
        log.warning(f"  Available: {self._files}")
        return "YMP_FILE_NOT_FOUND__" + "No file {} in Reference {}" "".format(
            filename, self.name
        ).replace(" ", "_")

    def generate_rules(self, **kwargs):
        for rsc in self._resources:
            yield from rsc.generate_rules(**kwargs)

    def __str__(self):
        return os.path.join(self.dir, "ALL")

    def this(self, args=None, kwargs=None):
        item = kwargs["item"]
        if kwargs.get("field") == "output":
            suffix = self.register_inout("this", set(), item).lstrip("/")
            self._files[suffix] = os.path.join(self.dir, suffix)
            self._outputs = None  # will need refresh
        return self.dir

    def prev(self, args=None, kwargs=None):
        return self.dir

    def minimize_variables(self, groups):
        """Removes redundant groupings

        This allows the reference to be used as a project, starting a pipeline"
        """
        if groups != []:
            raise YmpConfigError(self.cfg, "Reference may not be (re)grouped")
        return groups, []
