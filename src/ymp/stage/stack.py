"""
Implements the StageStack
"""

import logging
import copy

import ymp
from ymp.stage.stage import Stage
from ymp.stage.groupby import GroupBy
from ymp.exceptions import YmpStageError
from ymp.snakemake import ExpandLateException

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


def norm_wildcards(pattern):
    for n in ("{target}", "{source}", "{:target:}"):
        pattern = pattern.replace(n, "{sample}")
    for n in ("{:targets:}", "{:sources:}"):
        pattern = pattern.replace(n, "{:samples:}")
    return pattern


def find_stage(name):
    cfg = ymp.get_config()
    registry = Stage.get_registry()

    if name.startswith("group_"):
        return GroupBy(name)
    if name.startswith("ref_"):
        refname = name[4:]
        if refname in cfg.ref:
            return cfg.ref[refname]
        else:
            raise YmpStageError(f"Unknown reference '{cfg.ref[refname]}'")
    if name in cfg.projects:
        return cfg.projects[name]
    if name in cfg.pipelines:
        return cfg.pipelines[name]
    for stage in registry.values():
        if stage.match(name):
            return stage
    raise YmpStageError(f"Unknown stage '{name}'")


class StageStack(object):
    """The "head" of a processing chain - a stack of stages
    """

    used_stacks = set()

    @classmethod
    def instance(cls, path):
        """
        Cached access to StageStack

        Args:
          path: Stage path
          stage: Stage object at head of stack
        """
        cfg = ymp.get_config()
        cache = cfg.cache.get_cache(cls.__name__, itemloadfunc=StageStack)
        res = cache[path]
        if res not in cls.used_stacks:
            cls.used_stacks.add(res)
            res.show_info()
        return res

    def __str__(self):
        return self.path

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name}, {self.stage})"

    def __init__(self, path):
        #: Name of stack, aka is its full path
        self.name = path
        #: Names of stages on stack
        self.stage_names = path.split(".")
        #: Stages on stack
        self.stages = [find_stage(name)
                       for name in self.stage_names]
        #: Top Stage
        self.stage = self.stages[-1]
        #: Stage below top stage or None if first in stack
        self.prev_stage = self.stages[-2] if len(self.stages) > 1 else None

        cfg = ymp.get_config()

        #: Project on which stack operates
        #: This is needed for grouping variables currently.
        self.project = cfg.projects.get(self.stage_names[0])
        if not self.project:
            raise YmpStageError(f"No project for stage stack {path} found")

        #: Mapping of each input type required by the stage of this stack
        #: to the prefix stack providing it.
        self.prevs = self.resolve_prevs()

        # Gather all previous groups
        groups = list(dict.fromkeys(
            group
            for stack in reversed(list(self.prevs.values()))
            for group in stack.group
        ))
        project_groups, other_groups = self.project.minimize_variables(groups)
        # Check for groupby
        if isinstance(self.prev_stage, GroupBy):
            groupby = self.prev_stage.get_group(self, [], [])
        else:
            groupby = None
        self.group = self.stage.get_group(self, project_groups + other_groups, groupby)

    def show_info(self):
        log.info(
            "Stage stack '%s' (output by %s%s)",
            self,
            ", ".join(self.group) or "*ALL*",
            " + bins" if self.stage.has_checkpoint() else ""
        )
        prevmap = dict()
        for typ, stack in self.prevs.items():
            prevmap.setdefault(str(stack), []).append(typ)
        for stack, typ in prevmap.items():
            ftypes = ", ".join(typ).replace("/{sample}", "*")
            title = stack.split(".")[-1]
            if self.stage_names.count(title)  != 1:
                title = stack
            log.info(f"  input from {title}: {ftypes}")

    def resolve_prevs(self):
        inputs = self.stage.get_inputs()
        stage = self.stage
        prevs = self._do_resolve_prevs(stage, inputs, exclude_self=True)
        if inputs:
            raise YmpStageError(self._format_missing_input_error(inputs))
        return prevs

    def _format_missing_input_error(self, inputs):
        registry = Stage.get_registry()

        # Can't find the right types, try to present useful error message:
        words = []
        for item in inputs:
            words.extend((item, "--"))
            words.extend([name for name, stage in registry.items()
                          if stage.can_provide(set(item))])
            words.extend('\n')
        text = ' '.join(words)
        return f"""
        File type(s) '{" ".join(inputs)}' required by '{self.stage}'
        not found in '{self.name}'. Stages providing missing
        file types:
        {text}
        """

    def _do_resolve_prevs(self, stage, inputs, exclude_self):
        stage_names = copy.copy(self.stage_names)
        if exclude_self:
            stage_names.pop()

        prevs = {}
        while stage_names and inputs:
            path = ".".join(stage_names)
            prev_stage = find_stage(stage_names.pop())
            prev_stack = self.instance(path)
            provides = stage.satisfy_inputs(prev_stage, inputs)
            for typ, path in provides.items():
                if path:
                    path = ".".join(stage_names) + path
                    prev_stack = self.instance(path)
                prevs[typ] = prev_stack
        return prevs

    def complete(self, incomplete):
        registry = Stage.get_registry()
        cfg = ymp.get_config()
        result = []
        groups = ("group_" + name for name in self.project.variables + ['ALL'])
        result += (opt for opt in groups if opt.startswith(incomplete))
        refs = ("ref_" + name for name in cfg.ref)
        result += (opt for opt in refs if opt.startswith(incomplete))
        for stage in registry.values():
            for name in (stage.name, stage.altname):
                if name and name.startswith(incomplete):
                    try:
                        self.instance(".".join((self.path, name)))
                        result.append(name)
                    except YmpStageError:
                        pass

        return result

    @property
    def path(self):
        """On disk location of files provided by this stack"""
        return self.stage.get_path(self)

    def all_targets(self):
        return self.stage.get_all_targets(self)

    @property
    def defined_in(self):
        return None

    def prev(self, args=None, kwargs=None):
        """
        Directory of previous stage
        """
        if not kwargs or "wc" not in kwargs:
            raise ExpandLateException()

        item = kwargs.get('item')
        _, _, suffix = kwargs['item'].partition("{:prev:}")
        suffix = norm_wildcards(suffix)

        return self.prevs[suffix]

    @property
    def targets(self):
        """
        ID names of output files to be generated by current stage.
        """
        try:
            ids = self.stage.get_ids(self, self.group)
            log.error(f"Targets for {self}: {ids}")
            return ids
        except:
            log.exception("Unexpected!")
            import pdb; pdb.post_mortem()
            raise

    def target(self, args, kwargs):
        """
        ID names of input files for a specific input of the current stage
        """
        # Find stage stack from which input was generated
        prev_stack = self.prev(args, kwargs)
        # Find name of current output target
        cur_target = kwargs['wc'].target

        try:
            ids = self.stage.get_ids(self, prev_stack.group, self.group, cur_target)
            log.error(f"Prev targets for {self} and {prev_stack}/{cur_target}: {repr(ids)}")
            return ids
        except:
            log.exception("Unexpected 2!")
            import pdb; pdb.post_mortem()
            raise
