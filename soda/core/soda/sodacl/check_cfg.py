from __future__ import annotations

from abc import abstractmethod

from soda.sodacl.location import Location


class CheckCfg:
    def __init__(
        self,
        source_header: str,
        source_line: str,
        source_configurations: dict | None,
        location: Location,
        name: str | None,
    ):
        self.source_header: str = source_header
        self.source_line: str = source_line
        self.source_configurations = source_configurations
        self.location: Location = location
        self.name: str | None = name

    def get_column_name(self) -> str | None:
        pass

    @abstractmethod
    def get_identity_parts(self) -> list:
        pass
