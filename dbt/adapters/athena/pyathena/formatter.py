from copy import deepcopy
from decimal import Decimal
from typing import List, Optional

from pyathena.error import ProgrammingError

# noinspection PyProtectedMember
from pyathena.formatter import (
    _DEFAULT_FORMATTERS,
    Formatter,
    _escape_hive,
    _escape_presto,
)


class AthenaParameterFormatter(Formatter):
    def __init__(self) -> None:
        super().__init__(mappings=deepcopy(_DEFAULT_FORMATTERS), default=None)

    def format(self, operation: str, parameters: Optional[List[str]] = None) -> str:
        if not operation or not operation.strip():
            raise ProgrammingError("Query is none or empty.")
        operation = operation.strip()

        if operation.upper().startswith(("SELECT", "WITH", "INSERT")):
            escaper = _escape_presto
        elif operation.upper().startswith(("VACUUM", "OPTIMIZE")):
            operation = operation.replace('"', "")
        else:
            # Fixes ParseException that comes with newer version of PyAthena
            operation = operation.replace("\n\n    ", "\n")

            escaper = _escape_hive

        kwargs: Optional[List[str]] = None
        if parameters is not None:
            kwargs = list()
            if isinstance(parameters, list):
                for v in parameters:
                    # TODO Review this annoying Decimal hack, unsure if issue in dbt, agate or pyathena
                    if isinstance(v, Decimal) and v == int(v):
                        v = int(v)

                    func = self.get(v)
                    if not func:
                        raise TypeError(f"{type(v)} is not defined formatter.")
                    kwargs.append(func(self, escaper, v))
            else:
                raise ProgrammingError(f"Unsupported parameter (Support for list only): {parameters}")
        return (operation % tuple(kwargs)).strip() if kwargs is not None else operation.strip()
