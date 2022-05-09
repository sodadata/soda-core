from __future__ import annotations

from soda.execution.data_source import DataSource


def format_checks(checks: list, prefix: str = "", indent: int = 0, data_source: DataSource | None = None) -> str:
    indent_str = " " * indent
    checks_str = ""
    for check in checks:
        if isinstance(check, tuple):
            identifier = data_source.format_column_default(check[0]) if data_source else check[0]
            type = data_source.format_type_default(check[1]) if data_source else check[1]
            checks_str += f"{indent_str}{prefix} {identifier}: {type}\n"
        elif isinstance(check, str):
            identifier = data_source.format_column_default(check) if data_source else check
            checks_str += f"{indent_str}{prefix} {identifier}\n"

    return checks_str


def derive_schema_metric_value_from_test_table(test_table, data_source: DataSource):
    return [
        {
            "name": data_source.format_column_default(column[0]),
            "type": data_source.get_sql_type_for_schema_check(column[1]),
        }
        for column in test_table.columns
    ]
