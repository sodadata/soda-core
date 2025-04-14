from textwrap import dedent


def dedent_and_strip(text: str) -> str:
    return dedent(text).strip()
