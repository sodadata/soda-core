"""One verification request handed to the session impl, carrying its ``kind`` for registry dispatch."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from soda_core.common.yaml import CheckCollectionYamlSource


@dataclass(frozen=True)
class CheckCollectionSpec:
    """One verification request handed to the session impl.

    A session run takes a heterogeneous ``list[CheckCollectionSpec]`` — each
    spec carries its ``kind`` so the session impl can dispatch to the right
    family in the registry.
    """

    kind: str
    yaml_source: CheckCollectionYamlSource
    collection_name: Optional[str] = None
