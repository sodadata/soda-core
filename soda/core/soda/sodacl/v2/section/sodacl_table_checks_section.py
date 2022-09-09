from __future__ import annotations

from dataclasses import dataclass
from typing import List

from soda.sodacl.v2.check.sodacl_check import SodaClCheck
from soda.sodacl.v2.section.sodacl_section import SodaClSection


@dataclass
class SodaClTableChecksSection(SodaClSection):
    table_name: str
    partition_name: str | None
    sodacl_checks: list[SodaClCheck]
