"""
Implements the StageStack
"""

import logging
import copy
import re

from typing import List

import ymp
from ymp.stage.stage import Stage
from ymp.stage.groupby import GroupBy
from ymp.exceptions import YmpStageError
from ymp.snakemake import ExpandLateException

from snakemake.exceptions import IncompleteCheckpointException  # type: ignore

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


def norm_wildcards(pattern):
    pattern = re.sub(r"\{:\s*target(\(.*\))?\s*:\}", "{sample}", pattern)
    for pat in ("{target}", "{source}", "{:target:}"):
        pattern = pattern.replace(pat, "{sample}")
    for pat in ("{:targets:}", "{:sources:}"):
        pattern = pattern.replace(pat, "{:samples:}")
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
        raise YmpStageError(f"Unknown reference '{refname}'")
    if name.startswith(cfg.dir.references):
        refname = name[len(cfg.dir.references):].lstrip("/")
        if refname in cfg.ref:
            return cfg.ref[refname]
        raise YmpStageError(f"Unknown reference '{refname}'")
    if name in cfg.projects:
        return cfg.projects[name]
    for stage in registry.values():
        if stage.match(name):
            return stage
    for pipeline in cfg.pipelines.values():
        if pipeline.match(name):
            return pipeline
    raise YmpStageError(f"Unknown stage '{name}'")


class StageStack:
    """The "head" of a processing chain - a stack of stages
    """

    used_stacks = set()

    #: Set to true to enable additional Stack debug logging
    debug = False

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
        #: Top Stage Name
        self.stage_name = self.stage_names[-1]
        #: Stage below top stage or None if first in stack
        self.prev_stage = self.stages[-2] if len(self.stages) > 1 else None
        self.prev_stack = None
        if len(self.stages) > 1:
            self.prev_stack = self.instance(".".join(self.stage_names[:-1]))

        cfg = ymp.get_config()

        #: Project on which stack operates
        #: This is needed for grouping variables currently.
        self.project = cfg.projects.get(self.stage_names[0])
        if not self.project:
            if self.stage_names[0].startswith("ref_"):
                self.project = cfg.references.get(self.stage_names[0][4:])
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
        #: Grouping in effect for this StageStack. And empty list groups into
        #: one pseudo target, 'ALL'.
        self.group: List[str] = \
            self.stage.get_group(self, project_groups + other_groups)

    def show_info(self):
        def ellip(text: str) -> str:
            if len(text) < 40:
                return text
            return "..."+text[-37:]

        prevmap = dict()
        for typ, stack in self.prevs.items():
            prevmap.setdefault(str(stack), []).append(typ)
        log.info(
            "Stage stack '%s' (output by %s%s)",
            self.name,
            ", ".join(ellip(str(g)) for g in self.group) or "*ALL*",
            " + bins" if self.stage.has_checkpoint() else ""
        )
        for stack, typ in prevmap.items():
            ftypes = ", ".join(typ).replace("/{sample}", "*")
            title = stack.split(".")[-1]
            if self.stage_names.count(title) != 1:
                title = stack
            log.info("  input from %s: %s", title, ftypes)

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
            prev_stack = self.instance(path)
            prev_stage = find_stage(stage_names.pop())
            provides = stage.satisfy_inputs(prev_stage, inputs)
            for typ, ppath in provides.items():
                if ppath:
                    npath = prev_stage.get_path(prev_stack, typ, caller=self)
                    prevs[typ] = self.instance(npath)
                else:
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
        path = self.stage.get_path(self)
        while True:
            try:
                stack = self.instance(path)
            except YmpStageError:
                return path
            newpath = stack.stage.get_path(stack)
            if path == newpath:
                return newpath
            path = newpath

    def all_targets(self):
        return self.stage.get_all_targets(self)

    @property
    def defined_in(self):
        return None

    def prev(self, _args=None, kwargs=None) -> "StageStack":
        """
        Directory of previous stage
        """
        if not kwargs or "wc" not in kwargs:
            raise ExpandLateException()

        _, _, suffix = kwargs['item'].partition("{:prev:}")
        suffix = norm_wildcards(suffix)

        return self.prevs[suffix]

    def all_prevs(self, _args=None, kwargs=None) -> List["StageStack"]:
        if not kwargs or "wc" not in kwargs:
            raise ExpandLateException()

        _, _, suffix = kwargs['item'].partition("{:all_prevs:}")
        suffix = norm_wildcards(suffix)

        stage_names = copy.copy(self.stage_names)
        stage_names.pop()

        prevs = []
        while stage_names:
            path = ".".join(stage_names)
            prev_stack = self.instance(path)
            prev_stage = find_stage(stage_names.pop())
            ## FIXME: using prev_stack.stage instead of finding anew leads to deadlock?!
            pathlist = prev_stage.can_provide(set((suffix,)), full_stack = True).get(suffix, [])
            for ppath, hidden in pathlist:
                if ppath:
                    npath = prev_stage.get_path(prev_stack, pipeline=ppath)
                    prevs.append(self.instance(npath))
                else:
                    prevs.append(prev_stack)

        return prevs

    def get_ids(self, select_cols, where_cols=None, where_vals=None):
        if not self.debug:
            return self.stage.get_ids(self, select_cols, where_cols, where_vals)

        log.warning("  select %s", select_cols)
        log.warning("  where %s == %s", repr(where_cols), where_vals)
        try:
            ids = self.stage.get_ids(self, select_cols, where_cols, where_vals)
        except IncompleteCheckpointException as exc:
            log.warning(" ===> checkpoint deferred (%s)", exc.targetfile)
            raise
        log.warning("  ===> %s", repr(ids))
        return ids

    @property
    def targets(self):
        """
        Determines the IDs to be built by this Stage Stack
        (replaces "{:targets:}").
        """
        if self.debug:
            log.error("output ids for %s", self)
            log.warning("  select %s", repr(self.group))
        if self in self.group:
            group = self.group.copy()
            group.remove(self)
        else:
            group = self.group
        return self.get_ids(group)

    def target(self, args, kwargs):
        """
        Determines the IDs for a given input data type and output ID
        (replaces "{:target:}").
        """
        # Find stage stack from which input should be requested.
        # (not sure why the below causes a false positive in pylint)
        prev_stack = self.prev(args, kwargs)  # pylint: disable=not-callable
        # Find name of current output target
        cur_target = kwargs['wc'].target

        if self.debug:
            rulename = getattr(kwargs.get('rule'), 'name', 'N/A')
            log.error("input ids for %s", self)
            log.warning("  rule %s", rulename)
            log.warning("  from stack %s", prev_stack)
        cols = self.group
        vals = cur_target
        if cols == [] and vals == 'ALL':
            cols = vals = None

        ids = prev_stack.get_ids(prev_stack.group, cols, vals)

        if ids == []:
            rulename = getattr(kwargs.get('rule'), 'name', 'N/A')
            raise YmpStageError(
                f"Internal Error: Failed to find inputs\n\n"
                f"Context:\n"
                f"  In stack '{self}' rule '{rulename}'\n"
                f"  Building '{vals}' (grouped on '{','.join(cols)}')\n"
                f"  Seeking input from '{prev_stack}' "
                f"(grouped on '{','.join(prev_stack.group)}')"
                f"\n"
            )

        return ids
