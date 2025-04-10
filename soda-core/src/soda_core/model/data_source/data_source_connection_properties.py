from abc import ABC

from pydantic import BaseModel


class DataSourceConnectionProperties(
    BaseModel,
    ABC,
    frozen=True,
    extra="forbid",
    validate_by_name=True,  # Allow to use both field names and aliases when populating from dict
):
    pass
