import logging
import re

from ymp.exceptions import YmpRuleError
from ymp.snakemake import ColonExpander, ExpandLateException, RemoveValue
from ymp.string import PartialFormatter
from ymp.stage.stage import Stage
from ymp.stage.stack import StageStack

from snakemake.exceptions import IncompleteCheckpointException

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class StageExpander(ColonExpander):
    """
    - Registers rules with stages when they are created
    """
    regroup=re.compile("(?<!{){\s*([^{}\s]+)\s*}(?!})")

    def expand_ruleinfo(self, rule, item, expand_args, rec):
        stage = Stage.get_active()
        if not stage:
            return item

        stage.add_rule(rule, self.workflow)

        if not item.conda_env and stage.conda_env:
            item.conda_env = stage.conda_env

        if getattr(stage, "params", None):
            if not item.params:
                item.params = ((), {})
            for param in stage.params:
                item.params[1][param.name] = param.param_func()

        return super().expand_ruleinfo(rule, item, expand_args, rec)

    def expand_str(self, rule, item, expand_args, rec, cb):
        if cb:
            old_active = Stage.get_active()
            Stage.set_active(rule.ymp_stage)
        expand_args['item'] = item
        val = super().expand_str(rule, item, expand_args, rec, cb)
        if expand_args['field'] == 'message':
            val = self.regroup.sub("{wildcards.\\1}", val)
        if cb:
            Stage.set_active(old_active)
        return val

    def expands_field(self, field):
        return field not in 'func'

    class Formatter(ColonExpander.Formatter, PartialFormatter):
        # Careful here, TypeErrors are caught and hidden
        def get_value(self, key, args, kwargs):
            try:
                return self.get_value_(key, args, kwargs)
            except ExpandLateException:
                raise
            except IncompleteCheckpointException:
                raise
            except RemoveValue:
                raise
            except Exception as e:
                log.debug(
                    f"Formatter saw exception for"
                    f"key={key} args={args} kwargs={kwargs}",
                    exc_info=True)
                raise

        def get_value_(self, key, args, kwargs):
            stage = Stage.get_active()
            # Fixme: Guard against stage==None?
            if "(" in key:
                args = list(args)
                if key[-1] != ")":
                    raise YmpRuleError(
                        stage,
                        f"Malformed YMP expansion string:'{key}'")
                key, _, args_str = key[:-1].partition("(")
                for arg_str in args_str.split(","):
                    try:
                        arg = int(arg_str)
                    except ValueError:
                        try:
                            arg = float(arg_str)
                        except ValueError:
                            arg = arg_str
                    if isinstance(arg, str) and arg[0] not in ('"', '"'):
                        if "wc" not in kwargs:
                            raise ExpandLateException()
                        arg = getattr(kwargs['wc'], arg)
                    args += [arg]
                
            # Check Stage variables first. We can do that always:
            if hasattr(stage, key):
                val = getattr(stage, key)
                if hasattr(val, "__call__"):
                    val = val(args, kwargs)
                if val is not None:
                    return val

            # Check StageStack next. This requires the wildcards:
            if "wc" not in kwargs:
                raise ExpandLateException()
            wc = kwargs['wc']
            stack = StageStack.instance(stage.wc2path(wc))
            if hasattr(stack, key):
                val = getattr(stack, key)
                if hasattr(val, "__call__"):
                    val = val(args, kwargs)
                if val is not None:
                    return val

            # Check the project:
            if hasattr(stack.project, key):
                val = getattr(stack.project, key)
                if hasattr(val, "__call__"):
                    val = val(args, kwargs)
                if val is not None:
                    return val

            # Expand via super
            return  super().get_value(key, args, kwargs)
