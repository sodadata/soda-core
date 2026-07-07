from abc import ABC
from typing import Dict, List, Literal, Optional, Union

from duckdb import DuckDBPyConnection
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)


class ObjectStorageAssumeRoleAuth(BaseModel):
    """AWS credentials obtained by assuming an IAM role via STS AssumeRole."""

    model_config = ConfigDict(frozen=True)

    type: Literal["assume_role"] = Field("assume_role")
    role_arn: str = Field(..., description="ARN of the IAM role to assume", min_length=1)
    external_id: Optional[str] = Field(None, description="Optional STS ExternalId to pass to AssumeRole")


class ObjectStorageAccessKeyAuth(BaseModel):
    """Static AWS access-key credentials."""

    model_config = ConfigDict(frozen=True)

    type: Literal["access_key"] = Field("access_key")
    access_key_id: str = Field(..., description="AWS access key ID", min_length=1)
    secret_access_key: SecretStr = Field(..., description="AWS secret access key")
    session_token: Optional[str] = Field(None, description="Optional AWS session token")


# Polymorphic credential block, discriminated on the ``type`` key. Mirrors the
# Redshift AwsCredentials idioms (assume-role vs. static keys).
ObjectStorageAuth = Union[ObjectStorageAssumeRoleAuth, ObjectStorageAccessKeyAuth]


class ObjectStorageDataset(BaseModel):
    """A single object-storage dataset: one prefix+glob exposed as one view."""

    model_config = ConfigDict(frozen=True)

    name: str = Field(..., description="Dataset name; becomes the queryable view name", min_length=1)
    path: str = Field(
        ...,
        description="Object storage path/glob, e.g. s3://bucket/prefix/*.parquet (supports * and **)",
        min_length=1,
    )
    format: Literal["parquet", "csv", "json"] = Field(..., description="File format of the dataset")


class ObjectStorageProperties(BaseModel):
    """The ``object_storage`` connection block. Its presence marks a DuckDB data
    source as an object-storage (S3) source: each dataset is exposed as a view
    over files read via DuckDB ``httpfs``."""

    model_config = ConfigDict(frozen=True)

    provider: Literal["s3"] = Field("s3", description="Object storage provider. Only 's3' is implemented.")
    region: str = Field(..., description="AWS region of the bucket(s)", min_length=1)
    auth: ObjectStorageAuth = Field(..., discriminator="type", description="AWS credentials")
    datasets: List[ObjectStorageDataset] = Field(
        ..., description="Datasets to expose as views (each = one prefix+glob)", min_length=1
    )


class DuckDBConnectionProperties(DataSourceConnectionProperties, ABC):
    schema_: Optional[str] = Field(
        "main", description="Optional schema name to use for the DuckDB connection", alias="schema"
    )


class DuckDBStandardConnectionProperties(DuckDBConnectionProperties):
    database: str = Field(
        ":memory:", description="Path to the DuckDB database file or ':memory:' for in-memory database"
    )
    read_only: bool = Field(False, description="If True, the database is opened in read-only mode")
    configuration: Dict[str, str] = Field({}, description="Optional configuration dictionary for DuckDB")


class DuckDBObjectStorageConnectionProperties(DuckDBStandardConnectionProperties):
    """A DuckDB connection backed by object storage (S3). The ``database`` is
    typically ``:memory:``; each configured dataset is registered as an
    in-connection view over ``read_parquet`` / ``read_csv_auto`` / ``read_json_auto``."""

    object_storage: ObjectStorageProperties = Field(
        ..., description="Object storage (S3) configuration; its presence marks this an S3 source"
    )


class DuckDBExistingConnectionProperties(DuckDBConnectionProperties, arbitrary_types_allowed=True):
    duckdb_connection: Optional[DuckDBPyConnection] = Field(
        None, description="Optional existing DuckDB connection to use instead of creating a new one"
    )


class DuckDBDataSource(DataSourceBase, ABC):
    type: Literal["duckdb"] = Field("duckdb")
    connection_properties: DuckDBConnectionProperties = Field(
        ..., alias="connection", description="DuckDB connection configuration"
    )

    @field_validator("connection_properties", mode="before")
    @classmethod
    def infer_connection_type(cls, value):
        if isinstance(value, DuckDBConnectionProperties):
            return value

        # object_storage must be checked before database: the object-storage YAML
        # carries both keys (database ':memory:' + object_storage block).
        if "object_storage" in value:
            return DuckDBObjectStorageConnectionProperties(**value)
        elif "database" in value:
            return DuckDBStandardConnectionProperties(**value)
        elif "duckdb_connection" in value:
            return DuckDBExistingConnectionProperties(**value)
        raise ValueError("Could not infer DuckDB connection type from input")
