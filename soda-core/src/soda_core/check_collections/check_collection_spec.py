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

    Specs are intentionally unhashable — they describe per-``execute()`` inputs,
    not registry entries; use ``kind`` + identity for lookup. The frozen dataclass
    would otherwise auto-generate ``__hash__`` based on field hashability, and
    ``yaml_source`` (a ``CheckCollectionYamlSource``) may carry unhashable state.
    Setting ``__hash__ = None`` explicitly makes the unhashability deliberate
    rather than incidental — if a future code path actually needs to hash specs,
    the failure mode is clear and the fix should be at the call site, not by
    relying on whatever ``yaml_source`` happens to be hashable.
    """

    kind: str
    yaml_source: CheckCollectionYamlSource
    collection_name: Optional[str] = None
    __hash__ = None  # type: ignore[assignment]
