"""Tests for the DuckDB object-storage (S3) data source (DTL-1808).

The tests are split into:
  * pure config-parsing round-trips + validation error cases (hermetic);
  * view creation / glob / format machinery, proved against LOCAL temp files
    (no S3, no network) by calling the view-creation helper directly;
  * credential resolution: static access keys and STS AssumeRole (boto3 mocked),
    asserting the temp credentials flow into the DuckDB S3 secret;
  * the config-dict-ignored bug fix in the file-extension branch;
  * one end-to-end integration test through the full DataSourceImpl (requires the
    httpfs extension, skipped when it cannot be installed offline).
"""

from __future__ import annotations

from unittest import mock

import duckdb
import pandas as pd
import pytest
from pydantic import ValidationError
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.statements.table_types import TableType
from soda_core.common.yaml import DataSourceYamlSource
from soda_duckdb.common.data_sources.duckdb_data_source import (
    DuckDBDataSourceConnection,
    DuckDBDataSourceImpl,
)
from soda_duckdb.common.data_sources.duckdb_data_source_connection import (
    DuckDBDataSource as DuckDBDataSourceModel,
)
from soda_duckdb.common.data_sources.duckdb_data_source_connection import (
    DuckDBObjectStorageConnectionProperties,
    DuckDBStandardConnectionProperties,
    ObjectStorageAccessKeyAuth,
    ObjectStorageAssumeRoleAuth,
    ObjectStorageDataset,
    ObjectStorageProperties,
)


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #
class _RecordingConnection:
    """Minimal stand-in that records the SQL executed against it."""

    def __init__(self) -> None:
        self.executed: list[str] = []

    def execute(self, sql: str):
        self.executed.append(sql)
        return self


def _bare_connection_helper() -> DuckDBDataSourceConnection:
    """A DuckDBDataSourceConnection instance without opening a real connection.

    The object-storage helper methods only use class constants / other helpers,
    never per-instance connection state, so bypassing __init__ is safe here."""
    return DuckDBDataSourceConnection.__new__(DuckDBDataSourceConnection)


def _access_key_object_storage(datasets: list[ObjectStorageDataset]) -> ObjectStorageProperties:
    return ObjectStorageProperties(
        provider="s3",
        region="eu-west-1",
        auth=ObjectStorageAccessKeyAuth(access_key_id="AKIAEXAMPLE", secret_access_key="secretvalue"),
        datasets=datasets,
    )


def _httpfs_available() -> bool:
    try:
        connection = duckdb.connect(":memory:")
        connection.execute("INSTALL httpfs")
        connection.execute("LOAD httpfs")
        return True
    except Exception:
        return False


HTTPFS_AVAILABLE = _httpfs_available()


# --------------------------------------------------------------------------- #
# 1. Config parse round-trip                                                   #
# --------------------------------------------------------------------------- #
def _object_storage_dict(auth: dict) -> dict:
    return {
        "database": ":memory:",
        "object_storage": {
            "provider": "s3",
            "region": "eu-west-1",
            "auth": auth,
            "datasets": [
                {"name": "remittances", "path": "s3://highradius-landing/remittances/*.parquet", "format": "parquet"}
            ],
        },
    }


def test_parse_access_key_auth_round_trip():
    props = DuckDBObjectStorageConnectionProperties(
        **_object_storage_dict(
            {
                "type": "access_key",
                "access_key_id": "AKIAEXAMPLE",
                "secret_access_key": "topsecret",
                "session_token": "sometoken",
            }
        )
    )
    assert props.database == ":memory:"
    assert props.object_storage.provider == "s3"
    assert props.object_storage.region == "eu-west-1"
    assert isinstance(props.object_storage.auth, ObjectStorageAccessKeyAuth)
    assert props.object_storage.auth.access_key_id == "AKIAEXAMPLE"
    # Secret is wrapped in SecretStr and not exposed via repr.
    assert props.object_storage.auth.secret_access_key.get_secret_value() == "topsecret"
    assert "topsecret" not in repr(props.object_storage.auth)
    assert props.object_storage.auth.session_token == "sometoken"
    assert props.object_storage.datasets[0].name == "remittances"
    assert props.object_storage.datasets[0].format == "parquet"


def test_parse_assume_role_auth_round_trip():
    props = DuckDBObjectStorageConnectionProperties(
        **_object_storage_dict(
            {
                "type": "assume_role",
                "role_arn": "arn:aws:iam::123456789012:role/soda-reader",
                "external_id": "soda-highradius",
            }
        )
    )
    assert isinstance(props.object_storage.auth, ObjectStorageAssumeRoleAuth)
    assert props.object_storage.auth.role_arn == "arn:aws:iam::123456789012:role/soda-reader"
    assert props.object_storage.auth.external_id == "soda-highradius"


def test_assume_role_external_id_optional():
    props = DuckDBObjectStorageConnectionProperties(
        **_object_storage_dict({"type": "assume_role", "role_arn": "arn:aws:iam::123456789012:role/soda-reader"})
    )
    assert props.object_storage.auth.external_id is None


def test_object_storage_marks_connection_type_via_yaml():
    """Presence of the object_storage block routes to the object-storage props subclass."""
    model = DuckDBDataSourceModel(
        name="highradius_landing",
        connection={
            "database": ":memory:",
            "object_storage": {
                "provider": "s3",
                "region": "eu-west-1",
                "auth": {"type": "access_key", "access_key_id": "AK", "secret_access_key": "SK"},
                "datasets": [{"name": "r", "path": "s3://b/p/*.parquet", "format": "parquet"}],
            },
        },
    )
    assert isinstance(model.connection_properties, DuckDBObjectStorageConnectionProperties)


def test_standard_connection_still_infers_without_object_storage():
    model = DuckDBDataSourceModel(name="plain", connection={"database": ":memory:"})
    assert isinstance(model.connection_properties, DuckDBStandardConnectionProperties)
    assert not isinstance(model.connection_properties, DuckDBObjectStorageConnectionProperties)


# --------------------------------------------------------------------------- #
# 2. Validation error cases                                                    #
# --------------------------------------------------------------------------- #
def test_unsupported_provider_rejected():
    with pytest.raises(ValidationError) as exc_info:
        DuckDBObjectStorageConnectionProperties(
            database=":memory:",
            object_storage={
                "provider": "gcs",
                "region": "eu-west-1",
                "auth": {"type": "access_key", "access_key_id": "AK", "secret_access_key": "SK"},
                "datasets": [{"name": "r", "path": "gs://b/p/*.parquet", "format": "parquet"}],
            },
        )
    assert "provider" in str(exc_info.value)


def test_bad_format_rejected():
    with pytest.raises(ValidationError) as exc_info:
        ObjectStorageDataset(name="r", path="s3://b/p/*.avro", format="avro")
    assert "format" in str(exc_info.value)


def test_missing_access_key_fields_rejected():
    with pytest.raises(ValidationError):
        ObjectStorageAccessKeyAuth(access_key_id="AK")  # secret_access_key missing


def test_missing_role_arn_rejected():
    with pytest.raises(ValidationError):
        ObjectStorageAssumeRoleAuth(external_id="x")  # role_arn missing


def test_missing_region_rejected():
    with pytest.raises(ValidationError):
        ObjectStorageProperties(
            provider="s3",
            auth=ObjectStorageAccessKeyAuth(access_key_id="AK", secret_access_key="SK"),
            datasets=[ObjectStorageDataset(name="r", path="s3://b/p/*.parquet", format="parquet")],
        )


def test_empty_datasets_rejected():
    with pytest.raises(ValidationError):
        ObjectStorageProperties(
            provider="s3",
            region="eu-west-1",
            auth=ObjectStorageAccessKeyAuth(access_key_id="AK", secret_access_key="SK"),
            datasets=[],
        )


# --------------------------------------------------------------------------- #
# 3. View creation / glob / format (local temp files, no network)             #
# --------------------------------------------------------------------------- #
def test_create_views_parquet_glob(tmp_path):
    pd.DataFrame({"id": [1, 2]}).to_parquet(tmp_path / "part-0.parquet")
    pd.DataFrame({"id": [3]}).to_parquet(tmp_path / "part-1.parquet")
    datasets = [ObjectStorageDataset(name="remittances", path=str(tmp_path / "*.parquet"), format="parquet")]

    connection = duckdb.connect(":memory:")
    _bare_connection_helper()._create_object_storage_views(connection, datasets)

    # glob spans both files -> 3 rows
    assert connection.execute('SELECT count(*) FROM "remittances"').fetchone() == (3,)


def test_create_views_csv(tmp_path):
    (tmp_path / "data.csv").write_text("id,name\n1,a\n2,b\n")
    datasets = [ObjectStorageDataset(name="invoices", path=str(tmp_path / "data.csv"), format="csv")]

    connection = duckdb.connect(":memory:")
    _bare_connection_helper()._create_object_storage_views(connection, datasets)

    assert connection.execute('SELECT count(*) FROM "invoices"').fetchone() == (2,)


def test_create_views_json(tmp_path):
    (tmp_path / "data.json").write_text('[{"id": 1}, {"id": 2}, {"id": 3}]')
    datasets = [ObjectStorageDataset(name="events", path=str(tmp_path / "data.json"), format="json")]

    connection = duckdb.connect(":memory:")
    _bare_connection_helper()._create_object_storage_views(connection, datasets)

    assert connection.execute('SELECT count(*) FROM "events"').fetchone() == (3,)


def test_create_views_uses_correct_read_function():
    recording = _RecordingConnection()
    datasets = [
        ObjectStorageDataset(name="p", path="s3://b/p/*.parquet", format="parquet"),
        ObjectStorageDataset(name="c", path="s3://b/c/*.csv", format="csv"),
        ObjectStorageDataset(name="j", path="s3://b/j/*.json", format="json"),
    ]
    _bare_connection_helper()._create_object_storage_views(recording, datasets)
    sql = "\n".join(recording.executed)
    assert "read_parquet('s3://b/p/*.parquet')" in sql
    assert "read_csv_auto('s3://b/c/*.csv')" in sql
    assert "read_json_auto('s3://b/j/*.json')" in sql
    # view names are double-quoted identifiers
    assert 'CREATE OR REPLACE VIEW "p" AS' in sql


def test_view_identifier_and_path_are_escaped():
    helper = _bare_connection_helper()
    assert helper._quote_view_identifier('weird"name') == '"weird""name"'
    assert helper._escape_sql_literal("s3://b/o'brien/*.parquet") == "s3://b/o''brien/*.parquet"


# --------------------------------------------------------------------------- #
# 4. Discovery lists the created views (hermetic, no httpfs)                   #
# --------------------------------------------------------------------------- #
def test_discovery_lists_object_storage_views(tmp_path):
    pd.DataFrame({"id": [1, 2, 3]}).to_parquet(tmp_path / "r.parquet")
    (tmp_path / "c.csv").write_text("id\n1\n2\n")
    datasets = [
        ObjectStorageDataset(name="remittances", path=str(tmp_path / "r.parquet"), format="parquet"),
        ObjectStorageDataset(name="invoices", path=str(tmp_path / "c.csv"), format="csv"),
    ]

    raw_connection = duckdb.connect(":memory:")
    _bare_connection_helper()._create_object_storage_views(raw_connection, datasets)

    data_source_impl = DuckDBDataSourceImpl.from_existing_cursor(raw_connection, "os_discovery")
    data_source_impl.open_connection()
    discovered = data_source_impl.discover_qualified_objects(
        prefixes=["main"], object_types=[TableType.TABLE, TableType.VIEW]
    )
    discovered_names = sorted(obj.get_object_name() for obj in discovered)
    assert discovered_names == ["invoices", "remittances"]
    data_source_impl.close_connection()


def test_default_discovery_includes_object_storage_views(tmp_path):
    """The DEFAULT discovery path (no explicit object_types) must enumerate the
    object-storage VIEW datasets — that is the path the real product profiling /
    discovery flow uses. This goes through the full DataSourceImpl built from YAML,
    so the source is genuinely an object-storage source (connection_properties is
    DuckDBObjectStorageConnectionProperties). httpfs install + S3 secret creation
    are stubbed so the VIEWs are built over local files (no network, no httpfs)."""
    pd.DataFrame({"id": [1, 2, 3]}).to_parquet(tmp_path / "r.parquet")
    (tmp_path / "c.csv").write_text("id\n1\n2\n")

    yaml_str = f"""
        type: duckdb
        name: highradius_landing
        connection:
            database: ":memory:"
            object_storage:
                provider: s3
                region: eu-west-1
                auth:
                    type: access_key
                    access_key_id: "AK"
                    secret_access_key: "SK"
                datasets:
                    - name: remittances
                      path: "{tmp_path / 'r.parquet'}"
                      format: parquet
                    - name: invoices
                      path: "{tmp_path / 'c.csv'}"
                      format: csv
    """
    with mock.patch.object(DuckDBDataSourceConnection, "_install_httpfs"), mock.patch.object(
        DuckDBDataSourceConnection, "_create_s3_secret"
    ):
        data_source_impl = DataSourceImpl.from_yaml_source(DataSourceYamlSource.from_str(yaml_str=yaml_str))
        data_source_impl.open_connection()
        try:
            # NB: object_types is intentionally NOT passed -> the default path, which
            # used to default to TABLE-only and therefore miss the object-storage VIEWs.
            discovered = data_source_impl.discover_qualified_objects(prefixes=["main"])
            discovered_names = sorted(obj.get_object_name() for obj in discovered)
            assert discovered_names == ["invoices", "remittances"]
        finally:
            data_source_impl.close_connection()


def test_default_discovery_excludes_views_for_non_object_storage():
    """Scope guard: a standard (non-object-storage) DuckDB source keeps the TABLE-only
    default. A VIEW present in such a source is NOT returned by the default path; it is
    only surfaced when object_types explicitly asks for it. This proves the VIEW-inclusion
    is scoped to object-storage sources and does not regress local-file / standard DuckDB."""
    raw_connection = duckdb.connect(":memory:")
    raw_connection.execute("CREATE TABLE t AS SELECT 1 AS id")
    raw_connection.execute("CREATE VIEW v AS SELECT 1 AS id")

    data_source_impl = DuckDBDataSourceImpl.from_existing_cursor(raw_connection, "std_source")
    data_source_impl.open_connection()
    try:
        default_names = sorted(
            obj.get_object_name() for obj in data_source_impl.discover_qualified_objects(prefixes=["main"])
        )
        assert default_names == ["t"]  # view "v" excluded by the TABLE-only default

        with_views = sorted(
            obj.get_object_name()
            for obj in data_source_impl.discover_qualified_objects(
                prefixes=["main"], object_types=[TableType.TABLE, TableType.VIEW]
            )
        )
        assert with_views == ["t", "v"]  # mechanism still works when explicitly requested
    finally:
        data_source_impl.close_connection()


# --------------------------------------------------------------------------- #
# 5. Credential resolution -> DuckDB S3 secret                                 #
# --------------------------------------------------------------------------- #
def test_access_key_credentials_flow_into_secret():
    """Static access-key credentials populate a real DuckDB S3 secret."""
    object_storage = _access_key_object_storage(
        [ObjectStorageDataset(name="r", path="s3://b/p/*.parquet", format="parquet")]
    )
    connection = duckdb.connect(":memory:")
    _bare_connection_helper()._create_s3_secret(connection, object_storage)

    secrets = connection.execute("SELECT name, type FROM duckdb_secrets()").fetchall()
    assert (DuckDBDataSourceConnection.OBJECT_STORAGE_SECRET_NAME, "s3") in secrets


def test_assume_role_credentials_flow_into_secret():
    """STS AssumeRole temp credentials (mocked) flow into the CREATE SECRET statement,
    and ExternalId is passed through to STS."""
    object_storage = ObjectStorageProperties(
        provider="s3",
        region="eu-west-1",
        auth=ObjectStorageAssumeRoleAuth(
            role_arn="arn:aws:iam::123456789012:role/soda-reader", external_id="soda-highradius"
        ),
        datasets=[ObjectStorageDataset(name="r", path="s3://b/p/*.parquet", format="parquet")],
    )

    fake_sts = mock.Mock()
    fake_sts.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "ASIATEMPKEY",
            "SecretAccessKey": "tempsecretvalue",
            "SessionToken": "tempsessiontoken",
        }
    }
    recording = _RecordingConnection()

    with mock.patch("boto3.client", return_value=fake_sts) as client_mock:
        _bare_connection_helper()._create_s3_secret(recording, object_storage)

    client_mock.assert_called_once_with("sts", region_name="eu-west-1")
    fake_sts.assume_role.assert_called_once_with(
        RoleArn="arn:aws:iam::123456789012:role/soda-reader",
        RoleSessionName=DuckDBDataSourceConnection.OBJECT_STORAGE_ROLE_SESSION_NAME,
        ExternalId="soda-highradius",
    )
    secret_sql = "\n".join(recording.executed)
    assert "CREATE OR REPLACE SECRET" in secret_sql
    assert "ASIATEMPKEY" in secret_sql
    assert "tempsecretvalue" in secret_sql
    assert "tempsessiontoken" in secret_sql
    assert "REGION 'eu-west-1'" in secret_sql


def test_assume_role_without_external_id_omits_it():
    object_storage = ObjectStorageProperties(
        provider="s3",
        region="us-east-1",
        auth=ObjectStorageAssumeRoleAuth(role_arn="arn:aws:iam::123456789012:role/soda-reader"),
        datasets=[ObjectStorageDataset(name="r", path="s3://b/p/*.parquet", format="parquet")],
    )
    fake_sts = mock.Mock()
    fake_sts.assume_role.return_value = {
        "Credentials": {"AccessKeyId": "K", "SecretAccessKey": "S", "SessionToken": "T"}
    }
    with mock.patch("boto3.client", return_value=fake_sts):
        _bare_connection_helper()._create_s3_secret(_RecordingConnection(), object_storage)

    _, called_kwargs = fake_sts.assume_role.call_args
    assert "ExternalId" not in called_kwargs


def test_resolve_credentials_access_key_returns_secret_value():
    object_storage = ObjectStorageProperties(
        provider="s3",
        region="eu-west-1",
        auth=ObjectStorageAccessKeyAuth(access_key_id="AK", secret_access_key="SK", session_token="ST"),
        datasets=[ObjectStorageDataset(name="r", path="s3://b/p/*.parquet", format="parquet")],
    )
    key_id, secret, token = _bare_connection_helper()._resolve_object_storage_credentials(object_storage)
    assert (key_id, secret, token) == ("AK", "SK", "ST")


def test_duckdb_supports_create_secret_true_on_current_version():
    # Installed DuckDB is >= 1.x, well past the 0.10 Secrets API introduction.
    assert DuckDBDataSourceConnection._duckdb_supports_create_secret() is True


# --------------------------------------------------------------------------- #
# 6. Config-dict-ignored bug fix (file-extension branch)                       #
# --------------------------------------------------------------------------- #
def test_apply_configuration_sets_options():
    connection = duckdb.connect(":memory:")
    _bare_connection_helper()._apply_configuration(connection, {"memory_limit": "512MB"})
    # DuckDB normalizes the reported value; assert it was applied (not the default).
    applied = connection.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    assert "MiB" in applied or "GiB" in applied


def test_apply_configuration_handles_empty():
    connection = duckdb.connect(":memory:")
    # Should be a no-op and not raise.
    _bare_connection_helper()._apply_configuration(connection, {})
    _bare_connection_helper()._apply_configuration(connection, None)


def test_file_extension_branch_applies_configuration(tmp_path):
    """The file-extension branch used to ignore the configuration dict; it must
    now feed it through _apply_configuration."""
    parquet_path = tmp_path / "landing_probe.parquet"
    pd.DataFrame({"id": [1, 2, 3]}).to_parquet(parquet_path)
    yaml_str = f"""
        type: duckdb
        name: DUCKDB_CFG_PROBE
        connection:
            database: "{parquet_path}"
            configuration:
                memory_limit: "512MB"
    """
    with mock.patch.object(DuckDBDataSourceConnection, "_apply_configuration", autospec=True) as apply_mock:
        data_source_impl = DataSourceImpl.from_yaml_source(DataSourceYamlSource.from_str(yaml_str=yaml_str))
        data_source_impl.open_connection()
        data_source_impl.close_connection()

    assert apply_mock.called
    # autospec => (self, connection, configuration)
    passed_configuration = apply_mock.call_args.args[-1]
    assert passed_configuration == {"memory_limit": "512MB"}


# --------------------------------------------------------------------------- #
# 7. End-to-end through DataSourceImpl (requires httpfs)                        #
# --------------------------------------------------------------------------- #
@pytest.mark.skipif(not HTTPFS_AVAILABLE, reason="httpfs extension not available offline")
def test_object_storage_end_to_end_local_files(tmp_path):
    pd.DataFrame({"id": [1, 2]}).to_parquet(tmp_path / "a.parquet")
    pd.DataFrame({"id": [3]}).to_parquet(tmp_path / "b.parquet")
    (tmp_path / "c.csv").write_text("id\n10\n20\n30\n")

    yaml_str = f"""
        type: duckdb
        name: highradius_landing
        connection:
            database: ":memory:"
            object_storage:
                provider: s3
                region: eu-west-1
                auth:
                    type: access_key
                    access_key_id: "AKIAEXAMPLE"
                    secret_access_key: "secretexample"
                datasets:
                    - name: remittances
                      path: "{tmp_path / '*.parquet'}"
                      format: parquet
                    - name: invoices
                      path: "{tmp_path / 'c.csv'}"
                      format: csv
    """
    data_source_impl = DataSourceImpl.from_yaml_source(DataSourceYamlSource.from_str(yaml_str=yaml_str))
    data_source_impl.open_connection()
    try:
        assert data_source_impl.execute_query("SELECT count(*) FROM remittances").rows == [(3,)]
        assert data_source_impl.execute_query("SELECT count(*) FROM invoices").rows == [(3,)]
        discovered = data_source_impl.discover_qualified_objects(
            prefixes=["main"], object_types=[TableType.TABLE, TableType.VIEW]
        )
        assert sorted(obj.get_object_name() for obj in discovered) == ["invoices", "remittances"]
    finally:
        data_source_impl.close_connection()
