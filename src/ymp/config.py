import atexit
import glob
import logging
import os

from collections import OrderedDict
from xdg import XDG_CACHE_HOME  # type: ignore

from typing import Mapping, Sequence, Optional

import ymp.yaml
from ymp.common import AttrDict, MkdirDict, parse_number, format_number, parse_time, format_time
from ymp.cache import Cache, NoCache
from ymp.env import CondaPathExpander
from ymp.exceptions import YmpSystemError, YmpConfigError
from ymp.stage import Pipeline, Project, Reference
from ymp.snakemake import \
    BaseExpander, \
    ColonExpander, \
    DefaultExpander, \
    ExpandableWorkflow, \
    InheritanceExpander, \
    RecursiveExpander, \
    SnakemakeExpander, \
    get_workflow
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
    Override rule parameters, resources and threads using config values

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
                 resources:
                   memory: 15G
                 threads: 12
    """

    types = {
        "threads": int,
        "params": Mapping,
        "resources": Mapping,
    }

    def __init__(self, cfgmgr):
        if "overrides" not in cfgmgr._config:
            return
        self.rule_overrides = cfgmgr._config["overrides"].get("rules", {})
        super().__init__()

    def expand(self, rule, ruleinfo, **kwargs):
        overrides = self.rule_overrides.get(rule.name, {})
        for attr_name, values in overrides.items():
            if attr_name not in self.types:
                raise YmpConfigError(
                    overrides, f'Cannot override "{attr_name}" field', key=attr_name
                )
            attr = getattr(ruleinfo, attr_name)
            if not isinstance(values, self.types[attr_name]):
                raise YmpConfigError(
                    overrides,
                    f'Overrides for "{attr_name}" must be of type "{self.types[attr_name].__name__}"'
                    f' (found type "{type(values).__name__}").',
                    key=attr_name,
                )
            if isinstance(values, Mapping):
                if attr is None:
                    attr = ((), dict())
                    setattr(ruleinfo, attr_name, attr)
                for val_name, value in values.items():
                    log.debug(
                        "Overriding {}.{}={} in {} with {}".format(
                            attr_name, val_name, attr[1].get(val_name, "not set"), rule.name, value
                        )
                    )
                    attr[1][val_name] = value
            if isinstance(values, int):
                log.debug(
                    "Overriding {}={} in {} with {}".format(
                        attr_name, attr, rule.name, values
                    )
                )
                setattr(ruleinfo, attr_name, values)


class ResourceLimitsExpander(BaseExpander):
    """Allows adjusting resources to local compute environment

    Each config item defines processing for an item in ``resources:``
    or the special resource``threads``. Each item may have a
    ``default`` value filled in for rules not defining the resource,
    ``min`` and ``max`` defining the lower and uppeer bounds, and a
    ``scale`` value applied to the ``default`` to adjust resources up
    or down globally. Values in time or "human readable" format mabe
    parsed specially by passing the ``format`` values ``time`` or
    ``number``, respectively. These values will also be reformatted,
    with the optional paramter ``unit`` defining the output format
    (k/g/m/t for numbers and minutes/seconds for time). Additional
    resource values may be generated from configured onces using the
    ``from`` keyword (e.g. to provide both ``mem_mb`` and ``mem_gb``
    from a generic ``mem`` value.
    """

    parsers = {
        "number": parse_number,
        "time": parse_time,
    }
    formatters = {
        "number": format_number,
        "time": format_time,
    }

    def __init__(self, cfg: Optional[Mapping]) -> None:
        if not isinstance(cfg, Mapping):
            raise YmpConfigError(cfg, "Limits section must be a map (key: value)")
        self.limits = self.parse_config(cfg)
        log.debug("Parsed Resource Limits: %s", str(self.limits))

    def parse_config(self, cfg):
        """Parses limits config"""
        limits = OrderedDict()
        for name, params in cfg.items():
            lconf = {}
            format_name = params.get("format")
            lconf["parser"] = self.parsers.get(format_name) or (lambda x, unit=None: x)
            lconf["formatter"] = self.formatters.get(format_name) or (lambda x, unit=None: x)
            unit = params.get("unit")
            if unit:
                if not format:
                    raise YmpConfigError(cfg, 'Resource "unit" only valid with formatter', key=name)
                lconf["unit"] = unit
            source = params.get("from")
            if source:
                if source not in cfg:
                    raise YmpConfigError(
                        cfg,
                        f'Resource "from" ({source}) must reference'
                        f' previously defined resource (have {", ".join(cfg.keys())})',
                        key=name
                    )
                lconf["from"] = source
            for opt in params:
                if opt in ("default", "min", "max"):
                    try:
                        lconf[opt] = lconf['parser'](params.get(opt))
                    except ValueError:
                        raise YmpConfigError(
                            params,
                            f'Failed to parse "{params.get(opt)}"',
                            key=opt
                        ) from None
                elif opt in ("scale"):
                    lconf[opt] = params.get(opt)
                elif opt in ("format", "unit", "from"):
                    pass
                else:
                    raise YmpConfigError(
                        params,
                        f'Unknown parameter "{opt}" in "{name}" resource_limits',
                        opt
                    )
            limits[name] = lconf
        for key in list(limits.keys()):
            if limits[key].get("from"):
                limits.move_to_end(key)
        return limits

    def expands_field(self, field: str) -> bool:
        return field in ("threads", "resources")

    def expand(self, rule, ruleinfo, **kwargs) -> None:
        if ruleinfo.resources is None:
            ruleinfo.resources = ([], {})
        for rsrc, config in self.limits.items():
            if "from" in config:
                value = ruleinfo.resources[1][config["from"]]
            elif rsrc == "threads":
                value = ruleinfo.threads
            else:
                value = ruleinfo.resources[1].get(rsrc)

            if value is not None:
                value = config['parser'](value)
            value = self.adjust_value(
                value,
                config.get("default"),
                config.get("scale"),
                config.get("min"),
                config.get("max")
            )
            if value is not None:
                value = config['formatter'](value, unit = config.get("unit"))
                if rsrc == "threads":
                    ruleinfo.threads = value
                else:
                    ruleinfo.resources[1][rsrc] = value

    @staticmethod
    def adjust_value(
            value: Optional[int],
            default: Optional[int],
            scale: Optional[int],
            minimum: Optional[int],
            maximum: Optional[int],
    ) -> Optional[int]:
        """Applies default, scale, minimum and maximum to a numeric value)"""
        if value is None:
            if default is None:
                return None
            value = default
        elif scale is not None:
            value *= scale
        if minimum is not None and value < minimum:
            value = minimum
        if maximum is not None and value > maximum:
            value = maximum
        return value


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
    KEY_PIPELINES = 'pipelines'
    KEY_LIMITS = "resource_limits"
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
        try:
            curpath = os.path.abspath(os.getcwd())
        except FileNotFoundError:
            raise YmpSystemError("The current work directory has been deleted?!")
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
        Stage.set_active(None)

    def __init__(self, root, conffiles):
        log.debug("Inizializing ConfigMgr")
        self.root = root
        self.conffiles = conffiles

        if os.path.dirname(conffiles[-1]) == root:
            self.cachedir = os.path.join(self.root, ".ymp")
        else:
            self.cachedir = os.path.join(XDG_CACHE_HOME, "ymp")

        self._config = ymp.yaml.load(conffiles, root)
        self.cache = cache = NoCache(self.cachedir)

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

        self.pipelines = cache.get_cache(
            "pipelines",
            itemloadfunc=Pipeline,
            itemdata=self._config.get(self.KEY_PIPELINES) or {},
            dependfiles=conffiles
        )

        ExpandableWorkflow.register_expanders(
            SnakemakeExpander(),
            RecursiveExpander(),
            CondaPathExpander(self),
            StageExpander(),
            ConfigExpander(self),
            ResourceLimitsExpander(self._config.get(self.KEY_LIMITS)),
            OverrideExpander(self),
            InheritanceExpander(),
        )

    @property
    def workflow(self):
        return get_workflow()

    @property
    def rules(self):
        return AttrDict(self.workflow._rules)

    @property
    def ref(self):
        """
        Configure references
        """
        return self.references

    @property
    def pipeline(self):
        """
        Configure pipelines
        """
        return self.pipelines

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
        return self._config.directories.get_paths()

    @property
    def absdir(self):
        """
        Dictionary of absolute paths of named YMP directories
        """
        return self._config.directories.get_paths(absolute=True)

    @property
    def ensuredir(self):
        """
        Dictionary of absolute paths of named YMP directories

        Directories will be created on the fly as they are requested.
        """
        return MkdirDict(self.absdir)

    @property
    def cluster(self):
        """
        The YMP cluster configuration.
        """
        return self._config.cluster

    @property
    def snakefiles(self):
        """
        Snakefiles used under this config in parsing order
        """
        if not self._snakefiles:
            def find_files(dirname):
                if dirname is None:
                    return []
                listfiles =  glob.glob(os.path.join(dirname, "**", "*.rules"), recursive=True)
                listfiles.sort(key = lambda v: v.lower())
                listfiles = filter(lambda fname: os.path.basename(fname)[0] != ".", listfiles)
                return listfiles
            rule_dirs = self.absdir.rules.copy()
            snakefiles = []
            snakefiles.extend(find_files(rule_dirs.pop('builtin', None)))
            for dirname in rule_dirs:
                snakefiles.extend(find_files(rule_dirs[dirname]))
            self._snakefiles = snakefiles
        return self._snakefiles

    def expand(self, item, **kwargs):
        expander = ConfigExpander(self)
        res = expander.expand(None, item, kwargs)
        return res

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
