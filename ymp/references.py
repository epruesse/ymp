import logging
from typing import Optional, Dict
import os
from hashlib import sha1

from ymp.util import make_local_path
from ymp.snakemake import make_rule

log = logging.getLogger(__name__)


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
        return {fn: os.path.join(self.prefix, fn) for fn in self.files}

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
        self.dir = os.path.join(self.cfgmgr.dir.references,
                                self.name)

        for rsc in cfg:
            if isinstance(rsc, str):
                rsc = {'url': rsc}
            downloaded_path = make_local_path(self.cfgmgr, rsc['url'])
            type_name = rsc['type'].lower() if 'type' in rsc else 'fasta'
            if type_name == 'fasta':
                self.files['ALL.contigs.fasta.gz'] = downloaded_path
            elif type_name == 'fastp':
                self.files['ALL.contigs.fastp.gz'] = downloaded_path
            elif type_name == 'dir':
                archive = Archive(name=self.name,
                                  dirname=self.dir,
                                  tar=downloaded_path,
                                  url = rsc['url'],
                                  files=rsc['files'],
                                  strip=rsc.get('strip_components', 0))
                self.files.update(archive.get_files())
                self.archives.append(archive)
            else:
                log.debug("unknown type {} used in reference {}"
                          "".format(type_name, self.name))

    def get_file(self, filename):
        #log.debug("getting {}".format(filename))
        downloaded_path = self.files.get(filename)
        if downloaded_path:
            return downloaded_path
        #log.debug("Files in Ref {}: {}".format(self.name,
        #                                       "\n".join(self.files)))
        return ("YMP_FILE_NOT_FOUND__" +
                "No file {} in Reference {}"
                "".format(filename, self.name).replace(" ", "_"))

    def make_unpack_rules(self, baserule: 'Rule'):
        for archive in self.archives:
            yield archive.make_unpack_rule(baserule)


    # fixme remove?
    def __str__(self):
        return os.path.join(self.dir, "ALL.contigs")


def load_references(cfgmgr, cfg: Optional[dict]) -> Dict[str, Reference]:
    if not cfg:
        return {}
    references = {name: Reference(cfgmgr, name, data)
                  for name, data in cfg.items()}
    return references
