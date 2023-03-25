from __future__ import annotations

import re

from soda.contract.parser.parser_log import ParserLogs
from soda.contract.parser.parser_yaml import YamlString


def validate_name(logs: ParserLogs, yaml_string: YamlString | None) -> None:
    if yaml_string is not None:
        if '\n' in yaml_string.value or len(yaml_string.value) > 120:
            logs.error(
                message="Invalid name",
                location=yaml_string.location,
                docs_ref="02-data-producer-contract.md#string-types"
            )


email_regex = re.compile(r'([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+')


def validate_email(logs: ParserLogs, yaml_string: YamlString | None) -> None:
    if yaml_string is not None:
        if not re.fullmatch(cls.email_regex, yaml_string.value):
            logs.error(
                message="Invalid email",
                location=yaml_string.location,
                docs_ref="02-data-producer-contract.md#string-types"
            )


id_regex = re.compile(r'[A-Za-z0-9_]+')


def validate_id(logs: ParserLogs, yaml_string: YamlString | None) -> None:
    if yaml_string is not None:
        if not re.fullmatch(cls.id_regex, yaml_string.value):
            logs.error(
                message="Invalid id",
                location=yaml_string.location,
                docs_ref="02-data-producer-contract.md#string-types"
            )
