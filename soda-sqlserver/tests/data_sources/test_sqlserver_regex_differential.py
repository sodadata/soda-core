"""Differential test: the translated LIKE predicate must agree with Python's ``re``, on the engine.

This is the only test that has ever caught a real bug in the translator. Two other safety
nets did not:

- the unit tests, which assert the SHAPE of the generated LIKE and cannot know whether SQL
  Server agrees with that shape;
- a Python-side property test, which modelled LIKE semantics in Python -- so when the model
  was wrong, the translator and its oracle were wrong together and the test passed.

On 2026-10-29 that combination let four wrong-answer patterns through. SQL Server resolves a
LIKE range by COLLATION SORT ORDER, which interleaves case, so ``[a-z]`` also matched
``'AA'``. Only sending the predicate to a real server found it.

So: put the strings in a table, ask SQL Server which ones the predicate matches, ask Python's
``re`` which ones the pattern matches, and require the two sets to be identical. ``re`` is the
authority -- it defines what the contract author meant by the pattern.

Skips unless a live SQL Server is configured through the same environment variables the other
data-source tests use.
"""

from __future__ import annotations

import os
import re

import pytest
from helpers.test_connection import TestConnection

from soda_sqlserver.common.data_sources.regex_to_like import (
    RegexNotTranslatable,
    translate,
)

SQLSERVER_HOST = os.getenv("SQLSERVER_HOST", "localhost")
SQLSERVER_PORT = os.getenv("SQLSERVER_PORT", "1433")
SQLSERVER_DATABASE = os.getenv("SQLSERVER_DATABASE", "master")
SQLSERVER_USERNAME = os.getenv("SQLSERVER_USERNAME", "SA")
SQLSERVER_PASSWORD = os.getenv("SQLSERVER_PASSWORD")
SQLSERVER_DRIVER = os.getenv("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")
SQLSERVER_TRUST_SERVER_CERTIFICATE = os.getenv("SQLSERVER_TRUST_SERVER_CERTIFICATE", "true")

pytestmark = pytest.mark.skipif(
    not SQLSERVER_PASSWORD,
    reason="needs a live SQL Server; set SQLSERVER_PASSWORD and friends",
)

CONNECTION_YAML = f"""
    type: sqlserver
    name: SQLSERVER_REGEX_DIFFERENTIAL
    connection:
        host: '{SQLSERVER_HOST}'
        port: '{SQLSERVER_PORT}'
        database: '{SQLSERVER_DATABASE}'
        user: '{SQLSERVER_USERNAME}'
        password: '{SQLSERVER_PASSWORD}'
        driver: '{SQLSERVER_DRIVER}'
        trust_server_certificate: '{SQLSERVER_TRUST_SERVER_CERTIFICATE}'
"""

# The first nine are the country ZIP patterns from a real SAP T005; the rest probe constructs
# those nine happen not to use.
PATTERNS = [
    r"^\d{4}$",
    r"^\d{5}-\d{3}$",
    r"^[ABCEGHJ-NPRSTVXY]\d[ABCEGHJ-NPRSTV-Z][ ]?\d[ABCEGHJ-NPRSTV-Z]\d$",
    r"^(CH-)?[1-9]\d{3}$",
    r"^(D-)?\d{5}$",
    r"^[1-9][0-9]{5}$",
    r"^(0[1-9]\d{3}|[1-9]\d{4})$",
    r"^[1-9][0-9]{3}\s?(?!SA|SD|SS)[A-Z]{2}$",
    r"^\d{5}(?:[-\s]?\d{4})?$",
    r"^[A-Z]{2}\d{2}[A-Z0-9]{4}$",
    r"^\w{3}$",
    r"^[^0-9]{2}$",
    r"^a?b?c?$",
    r"^(AB|CD|EF)\d$",
    r"^\d{2}\.\d{2}$",
    r"^[0-9]{3}-[0-9]{2}-[0-9]{4}$",
    r"^(?!00)\d{2}$",
    r"^GB\d{2}[A-Z]{4}$",
    r"^[A-Z]\d[A-Z]$",
    r"^..$",
    r"^\D{2}\d{2}$",
    r"^(X|Y)(1|2)$",
    r"^[a-z]{2}$",
    r"^%\d{2}$",
    r"^_\d$",
    r"^\S{2}$",
    r"^[A-F0-9]{4}$",
    r"^(?!X)[A-Z]\d$",
]

# Deliberately heavy on case variants and near-misses: a translator bug lives at the boundary
# between matching and not matching, never in the middle of either.
SAMPLE_STRINGS = sorted(
    {
        "", " ", "  ", "0", "00", "000", "0000", "00000", "000000",
        "A", "AB", "ABC", "ABCD", "ZZ", "aB", "Ab", "ab", "aa", "abc", "AaA",
        "a", "b", "c", "bc", "ac", "0A", "A1B", "b9c", "z0A",
        "12345-6789", "12345 6789", "1234AB", "1234 AB", "1234SA", "1234 SA",
        "1234 Sz", "1234 zA", "CH-1234", "ch-1234", "D-12345", "d-12345",
        "AB12CD34", "aB12cd34", "Gz12ABCD", "zB12CD34", "GB12ABCD",
        "%12", "_1", "[1", "99", "1.23", "12.34", "123-45-6789",
        "000-00-0000", "X1", "Y2", "X2", "FF00", "ff00", "-", "--", "- ",
    }
)


def _lit(value: str) -> str:
    return "N'" + value.replace("'", "''") + "'"


@pytest.fixture(scope="module")
def data_source():
    connection = TestConnection(
        test_name="regex_differential", connection_yaml_str=CONNECTION_YAML
    )
    impl = connection.create_data_source_impl(connection.create_data_source_yaml())
    impl.open_connection()
    yield impl
    impl.close_connection()


@pytest.mark.parametrize("pattern", PATTERNS)
def test_translation_agrees_with_re_on_the_engine(data_source, pattern: str) -> None:
    try:
        likes, not_likes = translate(pattern)
    except RegexNotTranslatable:
        pytest.skip("refusals are a unit-test concern; this test is about what we accept")

    column = "v COLLATE SQL_Latin1_General_Cp1_CS_AS"
    matched = " OR ".join(f"{column} LIKE {_lit(p)}" for p in likes)
    predicate = f"({matched})" if len(likes) > 1 else matched
    for not_like in not_likes:
        predicate += f" AND {column} NOT LIKE {_lit(not_like)}"

    values = ",".join(f"({i},{_lit(s)})" for i, s in enumerate(SAMPLE_STRINGS))
    sql = (
        "DECLARE @t TABLE (i int, v nvarchar(50));\n"
        f"INSERT INTO @t (i, v) VALUES {values};\n"
        f"SELECT i FROM @t WHERE {predicate};"
    )

    from_engine = {int(row[0]) for row in data_source.execute_query(sql).rows}
    from_re = {i for i, s in enumerate(SAMPLE_STRINGS) if re.fullmatch(pattern, s)}

    assert from_engine == from_re, (
        f"LIKE and regex disagree for {pattern!r}\n"
        f"  matched by LIKE only:  {sorted(SAMPLE_STRINGS[i] for i in from_engine - from_re)}\n"
        f"  matched by regex only: {sorted(SAMPLE_STRINGS[i] for i in from_re - from_engine)}\n"
        f"  generated: LIKE {likes} NOT LIKE {not_likes}"
    )
