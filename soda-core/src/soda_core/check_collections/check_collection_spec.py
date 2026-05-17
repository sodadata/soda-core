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
    ``CheckCollection`` descriptor in the registry.

    ``collection_name`` is used as the path-prefix segment for multi-collection
    sessions: ``CheckImpl.path`` becomes ``f"{collection_name}.{raw_path}"``
    (or ``raw_path`` unchanged for contracts where collection_name is None).
    Backend ingestion recovers the originating collection identity via
    ``firstSegmentOf(checkPath)`` and looks it up by **name**, not UUID.

    For data standards (DES-317+) this MUST be ``data_standard.name`` (the
    customer-supplied slug, e.g. ``"pii"`` or ``"hipaa_basics"``), NOT the
    backend collection_id UUID — the backend's ``findByNames(prefixes,
    DataStandard.class)`` call at ``DataStandardIngestionFilterModule.java:62-74``
    only resolves on the ``name`` column.
    """

    kind: str
    yaml_source: CheckCollectionYamlSource
    collection_name: Optional[str] = None
