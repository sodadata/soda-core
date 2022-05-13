from dataclasses import dataclass
from typing import List


@dataclass
class SampleColumn:
    name: str
    type: str


@dataclass
class SampleSchema:
    columns: List[SampleColumn]
