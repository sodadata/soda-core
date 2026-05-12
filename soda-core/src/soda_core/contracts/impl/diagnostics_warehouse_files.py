from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Union


@dataclass(frozen=True)
class DiagnosticsWarehouseFiles:
    """File paths for diagnostics warehouse YAML configs.

    primary_path:
        Per-datasource DWH config. This is the pre-existing DWH target.
    metadata_path:
        Org-wide metadata DWH config. When set, a copy of selected metadata
        rows (currently check_results) is written here in addition to the
        primary target. When None, behavior is identical to the
        single-target era.
    metadata_schema:
        Optional schema name where the metadata DWH tables are created. When None,
        the metadata DWH's default schema is used.
    """

    primary_path: Optional[str] = None
    metadata_path: Optional[str] = None
    metadata_schema: Optional[str] = None

    @classmethod
    def normalize(
        cls, value: Optional[Union[str, "DiagnosticsWarehouseFiles"]]
    ) -> Optional["DiagnosticsWarehouseFiles"]:
        """Accept either a legacy single-path string or a DiagnosticsWarehouseFiles
        instance, and return a normalized DiagnosticsWarehouseFiles (or None).

        A bare string is treated as the primary path with no metadata target,
        preserving exact pre-existing behavior for string callers.
        """
        if value is None:
            return None
        if isinstance(value, str):
            return cls(primary_path=value)
        return value

    @property
    def is_empty(self) -> bool:
        return self.primary_path is None and self.metadata_path is None
