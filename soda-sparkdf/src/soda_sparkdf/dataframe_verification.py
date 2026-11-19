from __future__ import annotations

import logging
import re
import unicodedata
from typing import TYPE_CHECKING, Any, Optional, Union

from pyspark.errors import AnalysisException
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.exceptions import InvalidArgumentException
from soda_core.common.logging_constants import soda_logger
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.api.verify_api import verify_contract_locally
from soda_core.contracts.contract_verification import ContractVerificationSessionResult
from soda_core.telemetry.soda_tracer import soda_trace
from soda_sparkdf.common.data_sources.sparkdf_data_source import SparkDataFrameDataSourceImpl

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger: logging.Logger = soda_logger


@soda_trace
def verify_contract_on_dataframe(
    df: "DataFrame",
    *,
    data_sources: Union[list[DataSourceImpl], DataSourceImpl],
    publish_schema_metadata: bool,
    **verify_kwargs: Any,
) -> ContractVerificationSessionResult:
    """
    Verifies a Spark DataFrame against a contract and, with ``publish=True``, publishes the results to the contract's
    dataset in Soda Cloud.

    ``data_sources`` means what it means on ``verify_contract_locally``. It holds every data source of the run, as a
    list or one data source. One of them is a Spark DataFrame data source on the DataFrame's session, named like the
    contract's data source, for example
    ``SparkDataFrameDataSource.from_existing_session(df.sparkSession, name="databricks_prod")``. Other data sources,
    such as a reconciliation source, go in the same list.

    For the call the DataFrame is a temporary view of its session, ``soda_df_`` and the dataset's DQN as a slug.
    The helper refuses to run when a temporary view with that name exists, and leaves that view as it is. Every
    other keyword goes to ``verify_contract_locally``: ``contract_file_path`` or ``dataset_identifier``,
    ``publish``, ``soda_cloud_file_path`` and the rest. The helper sets ``execution_dataset`` itself.

    ``publish_schema_metadata`` decides whether the run sends the DataFrame's columns to Soda Cloud as the dataset's
    columns.
    """
    if "execution_dataset" in verify_kwargs:
        raise InvalidArgumentException(
            "verify_contract_on_dataframe sets execution_dataset to the DataFrame's view itself. To read another "
            "relation, call verify_contract_locally with execution_dataset"
        )
    sources: list = data_sources if isinstance(data_sources, list) else [data_sources]
    for data_source in sources:
        if not isinstance(data_source, DataSourceImpl):
            raise InvalidArgumentException(
                f"data_sources takes a data source or a list of data sources, as verify_contract_locally does, and "
                f"got a {type(data_source).__name__}"
            )
    if not any(
        isinstance(data_source, SparkDataFrameDataSourceImpl) and data_source.connection.session is df.sparkSession
        for data_source in sources
    ):
        raise InvalidArgumentException(
            "data_sources has no Spark DataFrame data source on the DataFrame's session. Add one named like the "
            "contract's data source, such as SparkDataFrameDataSource.from_existing_session(df.sparkSession, "
            "name='<the contract's data source>'). A temporary view belongs to one session, so a data source on "
            "another session can't read the DataFrame"
        )
    view: str = _view_name(verify_kwargs.get("dataset_identifier"), verify_kwargs.get("contract_file_path"))
    try:
        df.createTempView(view)
    except AnalysisException as e:
        # Classic Spark and Spark Connect both put the error class in the message.
        if "TEMP_TABLE_OR_VIEW_ALREADY_EXISTS" not in str(e):
            raise
        raise InvalidArgumentException(
            f"The temporary view {view} already exists in this Spark session. Another verification of the same "
            f"dataset on this session holds it, or the caller registered it. The helper left it in place. If no "
            f'verification holds it, drop it with spark.sql("DROP VIEW IF EXISTS `{view}`") and run again'
        ) from None
    try:
        return verify_contract_locally(
            data_sources=data_sources,
            execution_dataset=view,
            publish_schema_metadata=publish_schema_metadata,
            **verify_kwargs,
        )
    finally:
        try:
            # Not catalog.dropTempView: it also uncaches the DataFrame, so a df.cache() the caller set would be lost.
            df.sparkSession.sql(f"DROP VIEW IF EXISTS `{view}`")
        except Exception as e:
            logger.warning(f"Could not drop the temporary view {view}: {e}")


def _view_name(dataset_identifier: Optional[str], contract_file_path: Optional[str]) -> str:
    """soda_df_ and the dataset's DQN as a slug. One dataset gets the same view on every run, so two verifications of
    it on one session clash instead of sharing a view. Spark reads the name as an identifier, so the slug keeps only
    ASCII letters, digits and _."""
    dqn: Any = dataset_identifier
    if contract_file_path:
        # verify_contract_locally verifies the file when both are given, so the file names the view.
        # The dataset key as written. Its variables resolve only in the verification, and the name needs no values.
        dqn = ContractYamlSource.from_file_path(contract_file_path).parse().yaml_dict.get("dataset")
    if not isinstance(dqn, str) or not dqn:
        raise InvalidArgumentException(
            "verify_contract_on_dataframe names the DataFrame's view after the dataset, and found none. Pass "
            "dataset_identifier, or a contract_file_path whose contract has a dataset key"
        )
    ascii_dqn: str = unicodedata.normalize("NFKD", dqn).encode("ascii", "ignore").decode()
    slug: str = re.sub(r"[^a-z0-9]+", "_", ascii_dqn.lower()).strip("_")
    return f"soda_df_{slug}" if slug else "soda_df"
