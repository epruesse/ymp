import abc
import re
from typing import List, Dict, Type

from ymp.stage.base import BaseStage
from ymp.exceptions import YmpRuleError

class Param(abc.ABC):
    """Stage Parameter (base class)"""

    #: Type/Class mapping for param types
    types: Dict[str, "Type[Param]"] = {}

    #: Name of type, must be overwritten by children
    type_name: str = NotImplemented

    regex: str = NotImplemented

    def __init__(self, stage: BaseStage, key: str, name: str, value=None, default=None):
        self.stage = stage
        self.key = key
        self.name = name
        self.value = value
        self.default = default

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        if cls.type_name == NotImplemented:
            raise TypeError("Subclasses of Param must override 'type_name'")
        if cls.type_name in cls.types:
            raise TypeError(
                f"Type name '{cls.type_name}' already used by {cls.types[cls.type_name]}"
            )
        cls.types[cls.type_name] = cls

    @classmethod
    def make(cls, stage: BaseStage, typ: str, key: str, name: str, value, default) -> "Param":
        if typ not in cls.types:
            raise YmpRuleError(stage, f"Unknown stage Parameter type '{typ}'")
        return cls.types[typ](stage, key,name, value, default)

    @property
    def wildcard(self):
        return f"_yp_{self.name}"

    @property
    def constraint(self):
        if self.regex:
            return "," + self.regex
        return ""

    def pattern(self, show_constraint=True):
        """String to add to filenames passed to Snakemake

        I.e. a pattern of the form ``{wildcard,constraint}``
        """
        if show_constraint:
            return f"{{{self.wildcard}{self.constraint}}}"
        return f"{{{self.wildcard}}}"

    def param_func(self):
        """Returns function that will extract parameter value from wildcards"""
        def name2param(wildcards):
            # The part that matched our self.constraint will be in the
            # wildcard's field self.wildcard the parameter was matched
            val = wildcards.get(self.wildcard)
            if val:
                # Remove they key and return the matched portion
                return val[len(self.key):]
            # Return default value
            return self.default
        return name2param


class Parametrizable(BaseStage):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params: List[Param] = []
        self.__regex_ = None

    def add_param(self, key, typ, name, value=None, default=None) -> None:
        """Add parameter to stage

        Example:
            >>> with Stage("test") as S
            >>>   S.add_param("N", "int", "nval", default=50)
            >>>   rule:
            >>>      shell: "echo {param.nval}"

            This would add a stage "test", optionally callable as "testN123",
            printing "50" or in the case of "testN123" printing "123".

        Args:
          char: The character to use in the Stage name
          typ:  The type of the parameter (int, flag)
          param: Name of parameter in params
          value: value ``{param.xyz}`` should be set to if param given
          default: default value for ``{{param.xyz}}`` if no param given
        """
        self.params.append(Param.make(self, typ, key, name, value, default))

    @property
    def __regex(self):
        if not self.__regex_:
            regex = self.name + "".join(f"(?P<{param.wildcard}>{param.regex})" for param in self.params)
            self.__regex_ = re.compile(regex)
        return self.__regex_

    @property
    def regex(self):
        return self.name + "".join(param.regex for param in self.params)

    def parse(self, name: str) -> bool:
        match = self.__regex.fullmatch(name)
        groupdict = match.groupdict() if match else {}
        return {
            param.name: param.param_func()(groupdict)
            for param in self.params
        }

    def match(self, name: str) -> bool:
        return bool(self.__regex.fullmatch(name))


class ParamFlag(Param):
    """Stage Flag Parameter"""
    type_name = "flag"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.value:
            self.value = self.key

        self.regex = f"((?:{self.key})?)"

    def param_func(self):
        """Returns function that will extract parameter value from wildcards"""
        def name2param(wildcards):
            if getattr(wildcards, self.wildcard, None):
                return self.value
            return self.default or ""
        return name2param


class ParamInt(Param):
    """Stage Int Parameter"""
    type_name = "int"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.default is None:
            raise YmpRuleError(
                self.stage,
                "Stage Int Parameter must have 'default' set"
            )

        self.regex = f"({self.key}\\d+|)"


class ParamChoice(Param):
    """Stage Choice Parameter"""
    type_name = "choice"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.default is not None:
            self.value += [""]
        self.regex = f"({self.key}({'|'.join(self.value)}))"


class ParamRef(Param):
    """Reference Choice Parameter"""
    type_name = "ref"

    @property
    def regex(self):
        import ymp
        cfg = ymp.get_config()
        return f"({self.key}({'|'.join(cfg.references.keys())}))?"
