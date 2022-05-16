from dataclasses import dataclass
from typing import List


@dataclass
class SampleColumn:
    name: str
    type: str

    def get_cloud_dict(self):
        return {"name": self.name, "type": self.type}


@dataclass
class SampleSchema:
    columns: List[SampleColumn]
