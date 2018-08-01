import atexit
import glob
import logging
import os

from xdg import XDG_CACHE_HOME

import ymp.yaml
from ymp.common import AttrDict, Cache, MkdirDict, parse_number
from ymp.env import CondaPathExpander
from ymp.exceptions import YmpSystemError
from ymp.projects import Project
from ymp.references import Reference
from ymp.snakemake import \
    BaseExpander, \
    ColonExpander, \
    DefaultExpander, \
    ExpandableWorkflow, \
    InheritanceExpander, \
    RecursiveExpander, \
    SnakemakeExpander
from ymp.stage import StageExpander
from ymp.string import PartialFormatter

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class ConfigExpander(ColonExpander):
    def __init__(self, config_mgr):
        super().__init__()
        self.config_mgr = config_mgr

    def expands_field(self, field):
        return field not in 'func'

    class Formatter(ColonExpander.Formatter, PartialFormatter):
        def get_value(self, field_name, args, kwargs):
            cfg = self.expander.config_mgr

            # try to resolve variable as property of the config_mgr
            if hasattr(cfg, field_name):
                return getattr(cfg, field_name)
            return super().get_value(field_name, args, kwargs)


class OverrideExpander(BaseExpander):
    """
    Apply rule attribute overrides from ymp.yml config

    Example:
        Set the ``wordsize`` parameter in the `bmtagger_bitmask` rule to
        12:

        .. code-block:: yaml
           :caption: ymp.yml

           overrides:
             rules:
               bmtagger_bitmask:
                 params:
                   wordsize: 12
    """
    def __init__(self, cfgmgr):
        if 'overrides' not in cfgmgr._config:
            return
        self.rule_overrides = cfgmgr._config['overrides'].get('rules', {})
        super().__init__()

    def expand(self, rule, ruleinfo, **kwargs):
        overrides = self.rule_overrides.get(rule.name, {})
        for attr_name, values in overrides.items():
            attr = getattr(ruleinfo, attr_name)[1]
            for val_name, value in values.items():
                log.debug("Overriding {}.{}={} in {} with {}".format(
                    attr_name, val_name, attr[val_name], rule.name, value))
                attr[val_name] = value


class ConfigMgr(object):
    """Manages workflow configuration

    This is a singleton object of which only one instance should be around
    at a given time. It is available in the rules files as ``icfg`` and
    via `ymp.get_config()` elsewhere.

    ConfigMgr loads and maintains the workflow configuration as given
    in the ``ymp.yml`` files located in the workflow root directory,
    the user config folder (``~/.ymp``) and the installation ``etc``
    folder.
    """
    KEY_PROJECTS = 'projects'
    KEY_REFERENCES = 'references'
    CONF_FNAME = 'ymp.yml'
    CONF_DEFAULT_FNAME = ymp._defaults_file
    CONF_USER_FNAME = os.path.expanduser("~/.ymp/ymp.yml")
    RULE_MAIN_FNAME = ymp._snakefile

    __instance = None

    @classmethod
    def find_config(cls):
        """Locates ymp config files and ymp root

        The root ymp work dir is determined as the first (parent)
        directory containing a file named ``ConfigMgr.CONF_FNAME``
        (default ``ymp.yml``).

        The stack of config files comprises 1. the default config
        ``ConfigMgr.CONF_DEFAULT_FNAME`` (``etc/defaults.yml`` in the
        ymp package directory), 2. the user config
        ``ConfigMgr.CONF_USER_FNAME`` (``~/.ymp/ymp.yml``) and 3. the
        ``yml.yml`` in the ymp root.

        Returns:
          root: Root working directory
          conffiles: list of active configuration files
        """
        # always include defaults
        conffiles = [cls.CONF_DEFAULT_FNAME]

        # include user config if present
        if os.path.exists(cls.CONF_USER_FNAME):
            conffiles.append(cls.CONF_USER_FNAME)

        # try to find an ymp.yml in CWD and upwards
        filename = cls.CONF_FNAME
        log.debug("Locating '%s'", filename)
        curpath = os.path.abspath(os.getcwd())
        while not os.path.exists(os.path.join(curpath, filename)):
            log.debug("  not in '%s'", curpath)
            curpath, removed = os.path.split(curpath)
            if not removed:
                break
        if os.path.exists(os.path.join(curpath, filename)):
            root = curpath
            log.debug("  Found '%s' in '%s'", filename, curpath)
            conffiles.append(os.path.join(root, cls.CONF_FNAME))
        else:
            root = os.path.abspath(os.getcwd())
            log.debug("  No '%s' found; using %s as root", filename, root)

        return root, conffiles

    @classmethod
    def instance(cls):
        """Returns the active Ymp ConfigMgr instance

        """
        if cls.__instance is None:
            cls.__instance = cls(*cls.find_config())
        return cls.__instance

    @classmethod
    def activate(cls):
        ExpandableWorkflow.activate()

    @classmethod
    def unload(cls):
        log.debug("Unloading ConfigMgr")
        ExpandableWorkflow.clear()
        if cls.__instance:
            cls.__instance.cache.close()
        cls.__instance = None
        from ymp.stage import Stage, StageStack
        StageStack.stacks = {}
        Stage.active = None

    def __init__(self, root, conffiles):
        log.debug("Inizializing ConfigMgr")
        self.root = root
        self.conffiles = conffiles

        if os.path.dirname(conffiles[-1]) == root:
            self.cachedir = os.path.join(self.root, ".ymp")
        else:
            self.cachedir = os.path.join(XDG_CACHE_HOME, "ymp")

        self._config = ymp.yaml.load(conffiles)
        self.cache = cache = Cache(self.cachedir)

        # lazy filled by accessors
        self._snakefiles = None

        self.projects = cache.get_cache(
            "projects",
            itemloadfunc=Project,
            itemdata=self._config.get(self.KEY_PROJECTS) or {},
            dependfiles=conffiles
        )

        self.references = cache.get_cache(
            "references",
            itemloadfunc=Reference,
            itemdata=self._config.get(self.KEY_REFERENCES) or {},
            dependfiles=conffiles
        )

        self._workflow = ExpandableWorkflow.register_expanders(
            SnakemakeExpander(),
            RecursiveExpander(),
            CondaPathExpander(self),
            StageExpander(),
            ConfigExpander(self),
            OverrideExpander(self),
            DefaultExpander(params=([], {
                'mem': self.mem(),
                'walltime': self.limits.default_walltime
            })),
            InheritanceExpander(),
        )

    @property
    def ref(self):
        """
        Configure references
        """
        return self.references

    @property
    def pairnames(self):
        return self._config.pairnames

    @property
    def conda(self):
        return self._config.conda

    @property
    def dir(self):
        """
        Dictionary of relative paths of named YMP directories

        The directory paths are relative to the YMP root workdir.
        """
        return self._config.directories

    @property
    def absdir(self):
        """
        Dictionary of absolute paths of named YMP directories
        """
        return AttrDict({name: os.path.abspath(os.path.expanduser(value))
                         for name, value in self.dir.items()})

    @property
    def ensuredir(self):
        """
        Dictionary of absolute paths of named YMP directories

        Directories will be created on the fly as they are requested.
        """
        return MkdirDict({name: os.path.abspath(os.path.expanduser(value))
                         for name, value in self.dir.items()})

    @property
    def cluster(self):
        """
        The YMP cluster configuration.
        """
        return self._config.cluster

    @property
    def limits(self):
        """
        The YMP limits configuration.
        """
        return self._config.limits

    @property
    def snakefiles(self):
        """
        Snakefiles used under this config in parsing order
        """
        if not self._snakefiles:
            self._snakefiles = [
                fn
                for dn in (os.path.dirname(self.RULE_MAIN_FNAME),
                           self.absdir.rules)
                for fn in sorted(glob.glob(os.path.join(dn, "**", "*.rules"),
                                           recursive=True),
                                 key=lambda v: v.lower())
                if os.path.basename(fn)[0] != "."
            ]
        return self._snakefiles

    def expand(self, item, **kwargs):
        expander = ConfigExpander(self)
        res = expander.expand(None, item, kwargs)
        return res

    def mem(self, base="0", per_thread=None, unit="m"):
        """Clamp memory to configuration limits

        Params:
           base:       base memory requested
           per_thread: additional mem required per allocated thread
           unit:       output unit (b, k, m, g, t)
        """
        mem = parse_number(base)
        max_mem = parse_number(self.limits.max_mem)
        if mem > max_mem:
            mem = max_mem
        min_mem = parse_number(self.limits.min_mem)
        if mem < min_mem:
            mem = min_mem

        div = parse_number("1"+unit)

        return int(mem / div)

    @property
    def shell(self):
        """The shell used by YMP

        Change by adding e.g. ``shell: /path/to/shell`` to ``ymp.yml``.
        """
        return self._config.shell

    @property
    def platform(self):
        """Name of current platform (macos or linux)"""
        if not (hasattr(self, '_platform') and self._platform):
            import platform
            system = platform.system()
            if system == "Darwin":
                self._platform = "macos"
            elif system == "Linux":
                self._platform = "linux"
            else:
                raise YmpSystemError(f"YMP does not support system '{system}'")
        return self._platform


atexit.register(ConfigMgr.unload)
