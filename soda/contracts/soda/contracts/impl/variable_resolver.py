import os
import re


class VariableResolver:

    @classmethod
    def resolve(cls, text: str) -> str:
        return re.sub(
            pattern=r'\$\{([a-zA-Z_][a-zA-Z_0-9]*)\}',
            repl=lambda m: os.getenv(m.group(1).strip()),
            string=text
        )
