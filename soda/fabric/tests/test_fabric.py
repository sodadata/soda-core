from pathlib import Path
from helpers.data_source_fixture import DataSourceFixture
from helpers.data_source_fixture import TestTable

import logging


logger = logging.getLogger(__name__)


def test_integer_types(data_source_fixture: DataSourceFixture, tmp_path: Path):
    table_name = data_source_fixture.ensure_test_table(
        TestTable(
            name="NUMERIC_TYPES",
            columns=[
                ("c01", "smallint"),
                ("c02", "bigint"),
                ("c03", "integer"),
                ("c04", "bigint"),
                ("c05", "decimal"),
                ("c06", "numeric"),
                ("c07", "real"),
                ("c08", "double precision"),
            ],
            values=[
                (1, 1, 1, 1, 1, 1, 1, 1),
                (None, None, None, None, None, None, None, None)
            ],
        )
    )

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - missing_count(c01) = 1
            - missing_count(c02) = 1
            - missing_count(c03) = 1
            - missing_count(c04) = 1
            - missing_count(c05) = 1
            - missing_count(c06) = 1
            - missing_count(c07) = 1
            - missing_count(c08) = 1
        """
    )
    scan.execute()

    scan.assert_all_checks_pass()
    data_source_fixture._drop_test_table(table_name)


def test_string_types(data_source_fixture: DataSourceFixture, tmp_path: Path):
    table_name = data_source_fixture.ensure_test_table(
        TestTable(
            name="STRING_TYPES",
            columns=[
                ("c01", "varchar"),
                ("c02", "char"),
                ("c03", "text"),
            ],
            values=[
                ("a", "a", "a"),
                (None, None, None)
            ],
        )
    )

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - missing_count(c01) = 1
            - missing_count(c02) = 1
            - missing_count(c03) = 1
        """
    )
    scan.execute()

    scan.assert_all_checks_pass()
    data_source_fixture._drop_test_table(table_name)


def test_phone_numbers(data_source_fixture: DataSourceFixture, tmp_path: Path):
    table_name = data_source_fixture.ensure_test_table(
        TestTable(
            name="PHONE_NUMBER_CHECKS",
            columns=[
                ("phone_number", "varchar(255)"),
            ],
            values=[
                ('(112)460-2361x65894',),
                ('807-662-2501x35953',),
                ('(475)731-7388',),
                ('001-274-976-7914',),
                ('001-374-523-5313x4894',),
                ('001-890-029-5781x0453',),
                ('(104)288-2035',),
                ('(607)302-7399',),
                ('(888)525-5788',),
                ('001-091-463-6827x23608',),
                ('+1-719-180-2912',),
                ('4031776749',),
                ('(431)434-8587x558',),
                ('(527)557-4929',),
                ('632-820-4846',),
                ('293.284.8346x9240',),
                ('397-779-2835x593',),
                ('8609814597',),
                ('210-574-9910x971',),
                ('4520621059',)
            ],
        )
    )

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
              checks for {table_name}:
                - invalid_count(phone_number) = 0:
                    valid format: phone number
            """
    )
    scan.execute(allow_warnings_only=True)

    scan.assert_all_checks_pass()
    data_source_fixture._drop_test_table(table_name)


def test_email_addresses(data_source_fixture: DataSourceFixture, tmp_path: Path):
    table_name = data_source_fixture.ensure_test_table(
        TestTable(
            name="EMAIL_CHECKS",
            columns=[
                ("email_address", "varchar(255)"),
            ],
            values=[
                ('michelle09@example.org',),
                ('michael85@example.org',),
                ('fordjohn@example.net',),
                ('ehansen@example.com',),
                ('brownmorgan@example.net',),
                ('ymcdowell@example.net',),
                ('pittmanbrian@example.com',),
                ('ssnow@example.net',),
                ('irollins@example.org',),
                ('riggsjoseph@example.org',),
                ('vincent49@example.net',),
                ('meadowsrobert@example.net',),
                ('timothy82@example.net',),
                ('walkersusan@example.net',),
                ('hammondlaura@example.org',),
                ('ginajohnson@example.com',),
                ('masonmelinda@example.net',),
                ('wernerkelly@example.net',),
                ('samuel13@example.net',),
                ('moorekristin@example.com',)
            ],
        )
    )

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
              checks for {table_name}:
                - invalid_count(email_address) = 0:
                    valid format: email
            """
    )
    scan.execute()

    scan.assert_all_checks_pass()
    data_source_fixture._drop_test_table(table_name)


def test_duplicates(data_source_fixture: DataSourceFixture, tmp_path: Path):
    table_name = data_source_fixture.ensure_test_table(
        TestTable(
            name="DUPLICATES",
            columns=[
                ("c01", "int"),
            ],
            values=[
                (1,),
                (2,),
                (3,),
                (3,),
                (5,),
                (6,),
                (8,),
                (8,),
                (9,),
                (10,),
                (10,),
                (12,),
            ],
        )
    )

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - missing_count(c01) = 0
            - duplicate_count(c01) = 3
        """
    )
    scan.execute()

    scan.assert_all_checks_pass()
    data_source_fixture._drop_test_table(table_name)


def test_row_count(data_source_fixture: DataSourceFixture, tmp_path: Path):
    import random

    configured_row_count = random.randint(10, 100)

    values = []

    for i in range(configured_row_count):
        values.append(tuple([5]))

    table_name = data_source_fixture.ensure_test_table(
        TestTable(
            name="ROW_COUNT",
            columns=[
                ("c01", "int"),
            ],
            values=values,
        )
    )

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - row_count = {configured_row_count}
            - row_count between {configured_row_count - 10} and {configured_row_count + 10}
        """
    )
    scan.execute()

    scan.assert_all_checks_pass()
    data_source_fixture._drop_test_table(table_name)
