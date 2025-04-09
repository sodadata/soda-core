from abc import ABC

from pydantic import BaseModel


class DataSourceConnectionProperties(BaseModel, ABC):
    pass

    class Config:
        # Allow to use both field names and aliases when populating from dict
        validate_by_name = True
