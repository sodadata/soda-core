from __future__ import annotations

import os
import re
from typing import Dict

from soda.contracts.impl.logs import Logs


class VariableResolver:

    def __init__(self, logs: Logs | None = None, variables: Dict[str, str] | None = None):
        # See also adr/03_exceptions_vs_error_logs.md
        self.logs: Logs = logs if logs else Logs()
        self.variables: Dict[str, str] | None = variables

    def resolve(self, text: str) -> str:
        return re.sub(
            pattern=r"\$\{([a-zA-Z_][a-zA-Z_0-9]*)\}",
            repl=lambda m: self._resolve_variable(m.group(1).strip()),
            string=text,
        )

    def _resolve_variable(self, variable_name: str) -> str:
        if self.variables is not None and variable_name in self.variables:
            return self.variables[variable_name]
        if variable_name in os.environ:
            return os.getenv(variable_name)
        self.logs.error(f"Variable '{variable_name}' not defined in the variables nor as environment variable")
        return ""
