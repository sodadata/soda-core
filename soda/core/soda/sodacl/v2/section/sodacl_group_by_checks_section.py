from __future__ import annotations

from dataclasses import dataclass
from typing import List

from soda.sodacl.v2.section.sodacl_table_checks_section import SodaClTableChecksSection


@dataclass
class SodaClGroupByChecksSection(SodaClTableChecksSection):
    group_by_column_names: list[str]
