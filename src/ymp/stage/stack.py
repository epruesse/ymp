import logging
import copy

import ymp
from ymp.stage.stage import Stage
from ymp.stage.groupby import GroupBy
from ymp.exceptions import YmpStageError

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


def norm_wildcards(pattern):
    for n in ("{target}", "{source}", "{:target:}"):
        pattern = pattern.replace(n, "{sample}")
    for n in ("{:targets:}", "{:sources:}"):
        pattern = pattern.replace(n, "{:samples:}")
    return pattern


class StageStack(object):
    """The "head" of a processing chain - a stack of stages
    """

    used_stacks = set()

    @classmethod
    def get(cls, path, stage=None):
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
            log.info("Stage stack %s using column %s", res, res.group)
        return res

    def __str__(self):
        return self.path

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name}, {self.stage})"

    def __init__(self, path, stage=None):
        self.name = path
        self.stage_names = path.split(".")
        self.stages = [self._find_stage(name)
                       for name in self.stage_names]

        cfg = ymp.get_config()

        # determine project
        bottom_stage = self.stage_names[0]
        if bottom_stage in cfg.projects:
            self.project = cfg.projects[bottom_stage]
        elif bottom_stage in cfg.pipelines:
            self.project = cfg.pipelines[bottom_stage].project
        else:
            raise YmpStageError(f"No project for stage stack {path} found")

        # determine top stage
        stage_names = copy.copy(self.stage_names)
        top_stage = stage_names.pop()
        if stage:
            if not stage.match(top_stage):
                raise YmpStageError(
                    f"Internal error: {top_stage} not matched by {stage}")
        if not stage:
            stage = self._find_stage(top_stage)
        self.stage = stage

        # determine grouping
        self.group = getattr(stage, "group", None)
        if stage_names and stage_names[-1].startswith("group_"):
            self.group = [stage_names.pop().split("_")[1]]

        # collect inputs
        self.prevs = self.resolve_prevs()

        if self.group is None:
            groups = list(dict.fromkeys(
                group
                for p in reversed(list(self.prevs.values()))
                for group in p.group
            ))
            self.group = self.project.minimize_variables(groups)

        log.info("Stage stack %s using column %s", self, self.group)
        prevmap = dict()
        for typ, stack in self.prevs.items():
            prevmap.setdefault(str(stack), []).append(typ)
        for stack, typ in prevmap.items():
            ftypes = ", ".join(typ).replace("/{sample}", "*")
            title = stack.split(".")[-1]
            if self.stage_names.count(title)  != 1:
                title = stack
            log.info(f".. from {title}: {ftypes}")

    def resolve_prevs(self):
        inputs = self.stage.get_inputs()
        stage = self.stage

        prevs = self._do_resolve_prevs(stage, inputs, exclude_self=True)

        if inputs:
            registry = Stage.get_registry()

            # Can't find the right types, try to present useful error message:
            words = []
            for item in inputs:
                words.extend((item, "--"))
                words.extend([name for name, stage in registry.items()
                              if stage.can_provide(set(item))])
                words.extend('\n')
            text = ' '.join(words)

            raise YmpStageError(
                f"""
                File type(s) '{" ".join(inputs)}' required by '{self.stage}'
                not found in '{self.name}'. Stages providing missing
                file types:
                {text}
                """
            )
        return prevs

    def _do_resolve_prevs(self, stage, inputs, exclude_self):
        stage_names = copy.copy(self.stage_names)
        if exclude_self:
            stage_names.pop()

        prevs = {}
        while stage_names and inputs:
            path = ".".join(stage_names)
            prevstage = self._find_stage(stage_names.pop())
            if hasattr(prevstage, 'stagestack'):
                prevs.update(
                    prevstage.stagestack._do_resolve_prevs(
                        stage, inputs, exclude_self=False
                    )
                )
            else:
                prevstack = self.get(path, prevstage)
                provides = stage.satisfy_inputs(prevstage, inputs)
                for typ in provides:
                    prevs[typ] = prevstack
        return prevs

    def _find_stage(self, name):
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
        if name in cfg.pipelines:
            return cfg.pipelines[name]
        for stage in registry.values():
            if stage.match(name):
                return stage
        raise YmpStageError(f"Unknown stage '{name}'")

    def complete(self, incomplete):
        registry = Stage.get_registry()
        cfg = ymp.get_config()
        result = []
        groups = ("group_" + name for name in self.project.variables)
        result += (opt for opt in groups if opt.startswith(incomplete))
        refs = ("ref_" + name for name in cfg.ref)
        result += (opt for opt in refs if opt.startswith(incomplete))
        for stage in registry.values():
            for name in (stage.name, stage.altname):
                if name and name.startswith(incomplete):
                    try:
                        self.get(".".join((self.path, name)))
                        result.append(name)
                    except YmpStageError:
                        pass

        return result

    @property
    def path(self):
        # some stage types have fixed path
        return self.stage.get_path() or self.name

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
        Returns the current targets
        """
        return self.project.get_ids(self.group)

    def target(self, args, kwargs):
        """Finds the target in the prev stage matching current target"""
        prev_stage = self.prev(args, kwargs)
        prev = self.get(prev_stage.name)
        cur_target = kwargs['wc'].target
        try:
            target = self.project.get_ids(prev.group, self.group, cur_target)
        except e:
            log.exception("failed getting id")
            raise
        return target
