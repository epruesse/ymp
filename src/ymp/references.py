import logging
import os
from hashlib import sha1
from typing import Dict, Optional
from collections.abc import Mapping, Sequence

from ymp.snakemake import make_rule
from ymp.util import make_local_path

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class Archive(object):
    name = None
    hash = None
    tar = None
    dirname = None
    strip_components = None
    files = None

    def __init__(self, name, dirname, stage, tar, url, strip, files):
        self.name = name
        self.dirname = dirname
        self.stage = stage
        self.tar = tar
        self.url = url
        self.strip = strip
        self.files = files

        self.hash = sha1(self.tar.encode('utf-8')).hexdigest()[:8]
        self.prefix = os.path.join(self.dirname, "_unpacked_" + self.hash)

    def get_files(self):
        if isinstance(self.files, Sequence):
            return {self.stage + fn: os.path.join(self.prefix, fn)
                    for fn in self.files}
        elif isinstance(self.files, Mapping):
            return {self.stage + fn_ymp: os.path.join(self.prefix, fn_arch)
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


class Reference(object):
    """
    Represents (remote) reference file/database configuration
    """
    ONEFILE = "ALL.contigs"

    def __init__(self, reference, cfg):
        self.name = "ref_" + reference
        import ymp
        cfgmgr = ymp.get_config()
        self.cfg = cfg
        self.files = {}
        self.archives = []
        self.dir = os.path.join(cfgmgr.dir.references,
                                reference)

        for rsc in cfg:
            if isinstance(rsc, str):
                rsc = {'url': rsc}
            type_name = rsc.get('type', 'fasta').lower()
            stage = rsc.get("stage", "") + "/"
            downloaded_path = make_local_path(cfgmgr, rsc['url'])
            if type_name == 'fasta':
                self.files[stage + 'ALL.fasta.gz'] = downloaded_path
            elif type_name == 'fastp':
                self.files[stage + 'ALL.fastp.gz'] = downloaded_path
            elif type_name == 'gtf':
                self.files[stage + 'ALL.gtf'] = downloaded_path
            elif type_name == 'snp':
                self.files[stage + 'ALL.snp'] = downloaded_path
            elif type_name == 'dir':
                archive = Archive(name=self.name,
                                  dirname=self.dir,
                                  stage=stage,
                                  tar=downloaded_path,
                                  url=rsc['url'],
                                  files=rsc['files'],
                                  strip=rsc.get('strip_components', 0))
                self.files.update(archive.get_files())
                self.archives.append(archive)
            else:
                log.debug("unknown type {} used in reference {}"
                          "".format(type_name, self.name))

        self.outputs = set(f.replace("ALL", "{sample}") for f in self.files)
        self.inputs = set()
        self.group = ["ALL"]

    @property
    def defined_in(self):
        return self.cfg.get_files()

    def match(self, name):
        return name == self.name

    def get_file(self, filename, stage):
        downloaded_path = self.files.get(stage + "/" + filename)
        if downloaded_path:
            return downloaded_path
        return ("YMP_FILE_NOT_FOUND__" +
                "No file {} in Reference {}"
                "".format(filename, self.name).replace(" ", "_"))

    def make_unpack_rules(self, baserule: 'Rule'):
        for archive in self.archives:
            yield archive.make_unpack_rule(baserule)

    def __str__(self):
        return os.path.join(self.dir, "ALL")
