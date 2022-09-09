from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SodaClCheck:
    check_line: str
    check_configurations: dict | None
