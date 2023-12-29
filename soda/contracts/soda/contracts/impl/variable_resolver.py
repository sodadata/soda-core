from __future__ import annotations

import os
import re
from typing import Dict


class VariableResolver:

    @classmethod
    def resolve(cls, text: str, variables: Dict[str, str] | None = None) -> str:
        return re.sub(
            pattern=r'\$\{([a-zA-Z_][a-zA-Z_0-9]*)\}',
            repl=lambda m: cls._resolve_variable(m.group(1).strip(), variables),
            string=text
        )

    @classmethod
    def _resolve_variable(cls, variable_name: str, variables: Dict[str, str] | None = None) -> str:
        if variables is not None and variable_name in variables:
            return variables[variable_name]
        return os.getenv(variable_name)
