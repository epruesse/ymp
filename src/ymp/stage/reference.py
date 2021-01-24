import logging
import os
import re
from hashlib import sha1
from typing import Dict, Optional, Union, Set, List
from collections.abc import Mapping, Sequence

from ymp.snakemake import make_rule
from ymp.util import make_local_path
from ymp.stage import ConfigStage, Activateable, Stage

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class Archive(object):
    name = None
    hash = None
    tar = None
    dirname = None
    strip_components = None
    files = None

    def __init__(self, name, dirname, tar, url, strip, files):
        self.name = name
        self.dirname = dirname
        self.tar = tar
        self.url = url
        self.strip = strip
        self.files = files

        self.hash = sha1(self.tar.encode('utf-8')).hexdigest()[:8]
        self.prefix = os.path.join(self.dirname, "_unpacked_" + self.hash)

    def get_files(self):
        if isinstance(self.files, Sequence):
            return {fn: os.path.join(self.prefix, fn)
                    for fn in self.files}
        elif isinstance(self.files, Mapping):
            return {fn_ymp: os.path.join(self.prefix, fn_arch)
                    for fn_ymp, fn_arch in self.files.items()}
        else:
            raise Exception("unknown data type for reference.files")

    def make_unpack_rule(self, baserule: 'Rule'):
        docstr_tpl = """
        Unpacks {} archive:

        URL: {}

        Files:
        """

        item_tpl = """
        - {}
        """
        docstr = "\n".join([docstr_tpl.format(self.name, self.url)] +
                           [item_tpl.format(fn) for fn in self.files])
        return make_rule(
            name="unpack_{}_{}".format(self.name, self.hash),
            docstring=docstr,
            lineno=0,
            snakefile=__name__,
            parent=baserule,
            input=([], {'tar': self.tar}),
            output=([], {'files': list(self.get_files().values())}),
            params=([], {'strip': self.strip,
                         'prefix': self.prefix})
        )


class Reference(Activateable, ConfigStage):
    """
    Represents (remote) reference file/database configuration
    """
    def __init__(self, name, cfg):
        super().__init__("ref_" + name, cfg)
        #: Files provided by the reference. Keys are the file names
        #: within ymp ("target.extension"), values are the path to the
        #: reference file.
        self.files: Dict[str, str] = {}
        self.archives = []
        self._id = None
        self._outputs = None

        import ymp
        cfgmgr = ymp.get_config()

        self.dir = os.path.join(cfgmgr.dir.references, name)

        for rsc in cfg:
            if isinstance(rsc, str):
                rsc = {'url': rsc}
            self.add_files(rsc, make_local_path(cfgmgr, rsc['url']))

        # Copy rules defined in primary references stage
        self.rules = Stage.get_registry()['references'].rules.copy()

    def get_group(
            self,
            stack: "StageStack",
            default_groups: List[str]
    ) -> List[str]:
        return super().get_group(stack, [])

    def get_ids(
            self,
            stack: "StageStack",
            groups: List[str],
            match_groups: Optional[List[str]] = None,
            match_value: Optional[str] = None
    ) -> List[str]:
        if self._id:
            return [self._id]
        return super().get_ids(stack, groups, match_groups, match_value)

    @property
    def outputs(self) -> Union[Set[str], Dict[str, str]]:
        if self._outputs is None:
            self._outputs = {
                "/" + f.replace("ALL", "{sample}"): ""
                for f in self.files
            }
        return self._outputs

    def add_files(self, rsc, local_path):
        type_name = rsc.get('type', 'fasta').lower()

        if type_name == 'fasta':
            self.files['ALL.fasta.gz'] = local_path
        elif type_name == 'fastp':
            self.files['ALL.fastp.gz'] = local_path
        elif type_name == 'gtf':
            self.files['ALL.gtf'] = local_path
        elif type_name == 'snp':
            self.files['ALL.snp'] = local_path
        elif type_name == 'tsv':
            self.files['ALL.tsv'] = local_path
        elif type_name == 'csv':
            self.files['ALL.csv'] = local_path
        elif type_name == 'dir':
            archive = Archive(name=self.name,
                              dirname=self.dir,
                              tar=local_path,
                              url=rsc['url'],
                              files=rsc['files'],
                              strip=rsc.get('strip_components', 0))
            self.files.update(archive.get_files())
            self.archives.append(archive)
        elif type_name == 'dirx':
            self.files.update({
                key: local_path + val
                for key, val in rsc.get('files', {}).items()
            })
        elif type_name == 'path':
            self.dir = rsc['url'].rstrip('/')
            self._id = rsc.get('id')
            try:
                filenames = os.listdir(rsc['url'])
            except FileNotFoundError:
                log.error("Directory %s required by %s %s does not exist",
                          rsc['url'], self.__class__.__name__, self.name)
                filenames = []
            for filename in filenames:
                for regex in rsc.get('match', []):
                    match = re.fullmatch(regex, filename)
                    if not match:
                        continue
                    name = ''.join((
                        filename[:match.start('sample')],
                        'ALL',
                        filename[match.end('sample'):]
                    ))
                    self.files[name] = rsc['url'] + filename
        else:
            log.debug("unknown type {} used in reference {}"
                      "".format(type_name, self.name))

    def get_path(self, _stack):
        return self.dir

    def get_all_targets(self, stack: "StageStack") -> List[str]:
        return [os.path.join(self.dir, fname) for fname in self.files]

    def get_file(self, filename):
        local_path = self.files.get(filename)
        if local_path:
            return local_path
        log.error(f"{self.name}: Failed to find {filename}")
        log.warning(f"  Available: {self.files}")
        return ("YMP_FILE_NOT_FOUND__" +
                "No file {} in Reference {}"
                "".format(filename, self.name).replace(" ", "_"))

    def make_unpack_rules(self, baserule: 'Rule'):
        for archive in self.archives:
            yield archive.make_unpack_rule(baserule)

    def __str__(self):
        return os.path.join(self.dir, "ALL")

    def this(self, args=None, kwargs=None):
        item = kwargs['item']
        suffix = self.register_inout("this", set(), item).lstrip('/')
        self.files[suffix] = os.path.join(self.dir, suffix)
        self._outputs = None
        return self.dir

    def prev(self, args=None, kwargs=None):
        return self.dir
