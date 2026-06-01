from abc import ABC
from typing import Any, Literal, Optional, Union

from pydantic import Field, SecretStr, field_validator
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)


class SparkDataFrameConnectionProperties(DataSourceConnectionProperties, ABC):
    schema_: Optional[str] = Field(
        "main", description="Optional schema name to use for the SparkDataFrame connection", alias="schema"
    )
    test_dir: Optional[str] = Field(None, description="The directory to use for the test")
    use_catalog: bool = Field(
        False,
        description=(
            "When True, treat DWH prefixes as [catalog, schema] so DWH tables land in a "
            "Unity-Catalog-style 3-level namespace. The dialect emits CREATE CATALOG IF "
            "NOT EXISTS ahead of CREATE SCHEMA and qualifies all DDL/DML with the catalog. "
            "Leave False for local Spark which has no catalog concept."
        ),
    )


class SparkDataFrameNewSessionProperties(SparkDataFrameConnectionProperties):
    new_session: bool = Field(True, description="Whether to create a new Spark session")


class SparkDataFrameExistingSessionProperties(SparkDataFrameConnectionProperties, arbitrary_types_allowed=True):
    # We set the type to Any to avoid type errors when the SparkSession is not a SparkSession object
    # This could be the case on Databricks serverless, where the SparkSession is imported as a different object
    spark_session: Any = Field(..., description="The existing Spark session to use")


class SparkDataFrameRemoteSessionProperties(SparkDataFrameConnectionProperties):
    """SparkSession built via Spark Connect against a remote workspace (e.g. Databricks).

    The connection builds ``SparkSession.builder.remote(<uri>).getOrCreate()`` using a
    Spark Connect URI assembled from ``host``, ``token``, and ``cluster_id``. Because
    pyspark's Spark Connect builder caches sessions by URI per Python process, two
    DataSourceImpls configured against the same workspace+cluster share one underlying
    session — which is what we want for between-source DWH against Databricks.
    """

    host: str = Field(..., description="Workspace host (e.g. dbc-12345.cloud.databricks.com)")
    # SecretStr so the PAT renders as '**********' in repr/str — soda-core logs
    # connection_properties at DEBUG, which would otherwise leak the token verbatim
    # whenever verbose logging is on.
    token: SecretStr = Field(..., description="Personal access token, sent as gRPC bearer auth")
    cluster_id: str = Field(
        ...,
        description=(
            "All-purpose cluster id, forwarded as ``x-databricks-cluster-id`` gRPC metadata "
            "to route the Spark Connect session to a specific cluster"
        ),
    )


class SparkDataFrameActiveSessionProperties(SparkDataFrameConnectionProperties):
    """SparkSession picked up from ``SparkSession.getActiveSession()`` at connect time.

    Lets a DWH YAML reuse the SparkSession that's already active in the current thread —
    typically the Databricks notebook's ``spark``, or a session a caller built via
    ``SparkSession.builder.…getOrCreate()`` before invoking contract verification. No
    credentials in YAML, no module-level registry, no monkey-patching: pyspark already
    tracks the active session per thread and we just retrieve it.

    The connection raises a clear error when no active session is found.
    """

    use_active_session: Literal[True] = Field(
        ...,
        description=(
            "Must be ``true``. Discriminator for picking this connection mode in YAML; the "
            "session itself is fetched via SparkSession.getActiveSession() at connect time."
        ),
    )


class SparkDataFrameDataSource(DataSourceBase, ABC):
    type: Literal["sparkdf"] = Field("sparkdf")
    connection_properties: Union[
        SparkDataFrameExistingSessionProperties,
        SparkDataFrameRemoteSessionProperties,
        SparkDataFrameActiveSessionProperties,
        SparkDataFrameNewSessionProperties,
    ] = Field(..., alias="connection", description="SparkDataFrame connection configuration")

    @field_validator("connection_properties", mode="before")
    @classmethod
    def infer_connection_type(cls, value):
        if isinstance(value, SparkDataFrameNewSessionProperties):
            return value
        if isinstance(value, SparkDataFrameExistingSessionProperties):
            return value
        if isinstance(value, SparkDataFrameRemoteSessionProperties):
            return value
        if isinstance(value, SparkDataFrameActiveSessionProperties):
            return value

        if "spark_session" in value:
            return SparkDataFrameExistingSessionProperties(**value)
        elif value.get("use_active_session") is True:
            return SparkDataFrameActiveSessionProperties(**value)
        elif "host" in value and "cluster_id" in value:
            return SparkDataFrameRemoteSessionProperties(**value)
        elif "new_session" in value:
            return SparkDataFrameNewSessionProperties(**value)
        raise ValueError("Could not infer SparkDataFrame connection type from input")
