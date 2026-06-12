from os.path import dirname, exists
from pathlib import Path
from textwrap import dedent
from typing import Optional

from soda_core.cli.exit_codes import ExitCode
from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.yaml import DataSourceYamlSource


def handle_create_data_source(data_source_file_path: str, data_source_type: str) -> ExitCode:
    soda_logger.info(f"Creating {data_source_type} data source YAML file '{data_source_file_path}'")
    if exists(data_source_file_path):
        soda_logger.error(
            f"Could not create data source file '{data_source_file_path}'. "
            f"File already exists {Emoticons.POLICE_CAR_LIGHT}."
        )
        return ExitCode.LOG_ERRORS
    if data_source_type != "postgres":
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} Only type postgres is supported atm")
        return ExitCode.LOG_ERRORS
    dir_name = dirname(data_source_file_path)
    try:
        Path(dir_name).mkdir(parents=True, exist_ok=True)
        with open(data_source_file_path, "w") as text_file:
            text_file.write(
                dedent(
                    """
                type: postgres
                name: postgres_ds
                connection:
                    host: localhost
                    user: ${POSTGRES_USERNAME}
                    password: ${POSTGRES_PASSWORD}
                    database: your_postgres_db
                """
                ).strip()
            )
        soda_logger.info(f"{Emoticons.WHITE_CHECK_MARK} Created data source file '{data_source_file_path}'")
        return ExitCode.OK
    except Exception as exc:
        soda_logger.exception(f"An unexpected exception occurred: {exc}")
        return ExitCode.LOG_ERRORS


def handle_test_data_source(data_source_file_path: str) -> ExitCode:
    soda_logger.info(f"Testing data source configuration file {data_source_file_path}")
    from soda_core.common.data_source_impl import DataSourceImpl

    data_source_impl: DataSourceImpl = DataSourceImpl.from_yaml_source(
        DataSourceYamlSource.from_file_path(data_source_file_path)
    )
    error_message: Optional[str] = (
        data_source_impl.test_connection_error_message()
        if data_source_impl
        else "Data source could not be created. See logs above. Or re-run with -v"
    )
    if error_message:
        soda_logger.error(
            f"{Emoticons.POLICE_CAR_LIGHT} Could not connect using data source '{data_source_file_path}': "
            f"{error_message}"
        )
        return ExitCode.LOG_ERRORS
    else:
        soda_logger.info(f"{Emoticons.WHITE_CHECK_MARK} Success! Connection in '{data_source_file_path}' tested ok.")
        return ExitCode.OK


# Soda data type name (manifest string) -> SodaDataTypeName enum
def _soda_type(name: str):
    from soda_core.common.metadata_types import SodaDataTypeName

    return {
        "integer": SodaDataTypeName.INTEGER,
        "smallint": SodaDataTypeName.SMALLINT,
        "bigint": SodaDataTypeName.BIGINT,
        "varchar": SodaDataTypeName.VARCHAR,
        "char": SodaDataTypeName.CHAR,
        "text": SodaDataTypeName.TEXT,
        "decimal": SodaDataTypeName.DECIMAL,
        "numeric": SodaDataTypeName.NUMERIC,
        "double": SodaDataTypeName.DOUBLE,
        "float": SodaDataTypeName.FLOAT,
        "timestamp": SodaDataTypeName.TIMESTAMP,
        "timestamp_tz": SodaDataTypeName.TIMESTAMP_TZ,
        "date": SodaDataTypeName.DATE,
        "time": SodaDataTypeName.TIME,
        "boolean": SodaDataTypeName.BOOLEAN,
    }[name]


def _coerce(raw, soda_type):
    import datetime as _dt

    from soda_core.common.metadata_types import SodaDataTypeName

    if raw is None or raw == "":
        return None
    if soda_type in (SodaDataTypeName.INTEGER, SodaDataTypeName.SMALLINT, SodaDataTypeName.BIGINT):
        return int(raw)
    if soda_type in (SodaDataTypeName.DECIMAL, SodaDataTypeName.NUMERIC, SodaDataTypeName.DOUBLE, SodaDataTypeName.FLOAT):
        return float(raw)
    if soda_type in (SodaDataTypeName.TIMESTAMP, SodaDataTypeName.TIMESTAMP_TZ):
        return _dt.datetime.fromisoformat(raw)
    if soda_type == SodaDataTypeName.DATE:
        return _dt.date.fromisoformat(raw)
    if soda_type == SodaDataTypeName.TIME:
        return _dt.time.fromisoformat(raw)
    if soda_type == SodaDataTypeName.BOOLEAN:
        return str(raw).strip().lower() in ("true", "1", "yes", "t")
    return str(raw)


def handle_load_fixtures(
    fixture_name: str,
    data_source_file_path: str,
    schema: str,
    table_prefix: str,
) -> ExitCode:
    """Load a curated test fixture (CSV shipped with soda-core) into a sandbox
    schema of a data source, via CREATE TABLE + INSERT through the SQL AST.

    Safety: writes ONLY into `schema`, and only ever drops `<table_prefix>*`
    tables. Intended for an isolated sandbox schema (e.g. _soda_test)."""
    import csv as _csv
    from pathlib import Path

    from ruamel.yaml import YAML

    from soda_core.common.data_source_impl import DataSourceImpl
    from soda_core.common.metadata_types import SqlDataType
    from soda_core.common.sql_ast import (
        CREATE_TABLE_COLUMN,
        CREATE_TABLE_IF_NOT_EXISTS,
        DROP_TABLE_IF_EXISTS,
        INSERT_INTO,
        LITERAL,
        VALUES_ROW,
    )

    if not table_prefix:
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} --table-prefix is required (sandbox safety)")
        return ExitCode.LOG_ERRORS

    fixtures_dir = Path(__file__).resolve().parent.parent / "fixtures"
    manifest_path = fixtures_dir / f"{fixture_name}.yml"
    if not manifest_path.exists():
        available = sorted(p.stem for p in fixtures_dir.glob("*.yml"))
        soda_logger.error(
            f"{Emoticons.POLICE_CAR_LIGHT} Unknown fixture '{fixture_name}'. Available: {', '.join(available)}"
        )
        return ExitCode.LOG_ERRORS
    manifest = YAML(typ="safe").load(manifest_path.read_text())
    csv_path = fixtures_dir / manifest["csv"]

    data_source_impl: DataSourceImpl = DataSourceImpl.from_yaml_source(
        DataSourceYamlSource.from_file_path(data_source_file_path)
    )
    if data_source_impl is None:
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} Could not create data source. Re-run with -v")
        return ExitCode.LOG_ERRORS

    dialect = data_source_impl.sql_dialect
    db = getattr(getattr(data_source_impl.data_source_model, "connection", None), "database", None)
    prefix = [db, schema]

    data_source_impl.open_connection()
    try:
        # 1. Sandbox schema
        data_source_impl.execute_update(dialect.create_schema_if_not_exists_sql(prefix))

        # 2. Columns from the manifest (no type inference)
        type_name_by_soda = dialect.get_data_source_data_type_name_by_soda_data_type_names()
        columns = []
        soda_types = {}
        for c in manifest["columns"]:
            st = _soda_type(c["type"])
            soda_types[c["name"]] = st
            columns.append(
                CREATE_TABLE_COLUMN(
                    name=c["name"],
                    type=SqlDataType(
                        name=type_name_by_soda[st],
                        character_maximum_length=c.get("length"),
                        numeric_precision=c.get("precision"),
                        numeric_scale=c.get("scale"),
                        datetime_precision=c.get("datetime_precision"),
                    ),
                    nullable=True,
                )
            )
        table = f"{table_prefix}{manifest['table']}"
        fq = dialect.qualify_dataset_name(prefix, table)

        # 3. Recreate the table (drop is scoped to the prefixed name only)
        data_source_impl.execute_update(dialect.build_drop_table_sql(DROP_TABLE_IF_EXISTS(fully_qualified_table_name=fq)))
        data_source_impl.execute_update(
            dialect.build_create_table_sql(CREATE_TABLE_IF_NOT_EXISTS(fully_qualified_table_name=fq, columns=columns))
        )

        # 4. Read CSV -> VALUES_ROWs (column order from the manifest)
        order = [c["name"] for c in manifest["columns"]]
        std_columns = [c.convert_to_standard_column() for c in columns]
        values_rows = []
        with open(csv_path, newline="") as f:
            for r in _csv.DictReader(f):
                values_rows.append(VALUES_ROW([LITERAL(_coerce(r[name], soda_types[name])) for name in order]))

        # 5. Batch INSERT
        batch = 5000
        for i in range(0, len(values_rows), batch):
            data_source_impl.execute_update(
                dialect.build_insert_into_sql(
                    INSERT_INTO(fully_qualified_table_name=fq, values=values_rows[i : i + batch], columns=std_columns)
                )
            )

        # 6. Verify
        result = data_source_impl.execute_query(f"SELECT COUNT(*) FROM {fq}")
        loaded = result.rows[0][0] if getattr(result, "rows", None) else "?"
        soda_logger.info(
            f"{Emoticons.WHITE_CHECK_MARK} Loaded fixture '{fixture_name}' into {fq} "
            f"({loaded} rows, {len(columns)} columns)"
        )
        return ExitCode.OK
    except Exception as exc:
        soda_logger.exception(f"{Emoticons.POLICE_CAR_LIGHT} Failed to load fixture: {exc}")
        return ExitCode.LOG_ERRORS
    finally:
        data_source_impl.close_connection()
