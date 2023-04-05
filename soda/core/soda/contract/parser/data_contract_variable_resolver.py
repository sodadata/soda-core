from __future__ import annotations

import logging
import os
import re


class DataContractVariableResolver:
    """
    Resolves dynamic content like variables and functions in files.
    """

    def __init__(self, variables: dict[str, str] = os.environ):
        self.variables = variables

    curly_braces_regex = re.compile(r"\${( *[a-zA-Z_]+[a-zA-Z0-9_(),.\']* *)}")

    def resolve_variables(self, text: str) -> str:
        def repl(match):
            variable_text = match.group(1)
            expression = variable_text.strip()
            value = self._get_variable_value(expression)
            if isinstance(value, str):
                logging.debug(f"Replacing variable {expression} with {value}")
                return value
            else:
                source = f"${{{variable_text}}}"
                logging.debug(f"Ignoring unknown variable {source}")
                return source

        return re.sub(pattern=self.curly_braces_regex, repl=repl, string=text)

    def _get_variable_value(self, variable_name: str) -> str | None:
        return self.variables.get(variable_name)
