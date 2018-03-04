import logging
from typing import Optional
import os

from ymp.util import make_local_path

log = logging.getLogger(__name__)


class Reference(object):
    """
    Represents (remote) reference file/database configuration
    """
    def __init__(self, cfgmgr, reference, cfg):
        self.name = reference
        self.cfgmgr = cfgmgr
        self.cfg = cfg
        self.files = {}
        self.archives = []
        for rsc in cfg:
            if isinstance(rsc, str):
                log.error(self.name)
            downloaded_path = make_local_path(self.cfgmgr, rsc['url'])
            type_name = rsc['type'].lower() if 'type' in rsc else 'fasta'
            if type_name == 'fasta':
                self.files['ALL.contigs.fasta.gz'] = downloaded_path
            elif type_name == 'fastp':
                self.files['ALL.contigs.fastp.gz'] = downloaded_path
            elif type_name == 'dir':
                files = [
                    os.path.join(
                        self.cfgmgr.dir.references,
                        self.name,
                        "_unpacked_{}".format(len(self.archives)),
                        fn)
                    for fn in rsc['files']
                ]
                for fn in rsc['files']:
                    self.files[fn] = files
                strip_components = rsc.get('strip_components', 0)
                self.archives.append((downloaded_path, strip_components))
            else:
                log.debug("unknown type {} used in reference {}"
                          "".format(type_name, self.name))

    def __str__(self):
        res = "{refdir}/{refname}/ALL.contigs".format(
            refdir=self.cfgmgr.dir.references,
            refname=self.name
        )
        return res

    def get_file(self, filename):
        log.debug("getting {}".format(filename))
        downloaded_path = self.files.get(filename)
        if downloaded_path:
            return downloaded_path
        log.debug("Files in Ref {}: {}".format(self.name,
                                               "\n".join(self.files)))
        return ("YMP_FILE_NOT_FOUND__" +
                "No file {} in Reference {}"
                "".format(filename, self.name).replace(" ", "_"))

    def get_archive(self, number):
        return self.archives[int(number)][0]

    def get_strip(self, number):
        return self.archives[int(number)][1]

    @property
    def dir(self):
        return os.path.join(self.cfgmgr.dir.references, self.name)


def load_references(cfgmgr, cfg: Optional[dict]) -> dict[str, Reference]:
    if not cfg:
        return {}
    return {name: Reference(cfgmgr, name, data)
            for name, data in cfg.items()}
