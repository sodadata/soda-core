# Unit tests

Tests are grouped together by feature, especially in the `verification` package.  Each check type has it's
test file.

## Test tables

Test tables are upserted and only recreated if something in the table changed.  This is done for speed of
development so that iterative running of the same tests with the same table doesn't need to drop and recreate.

Test tables should only be used in the same file as they are declared.  That way changing test tables should
only affect a single file.

Eg

```python
contracts_invalid_test_table = TestTable(
    name="contracts_invalid",
    # fmt: off
    columns=[
        ("one", DataType.TEXT)
    ],
    values=[
        ('ID1',),
        ('XXX',),
        ('N/A',),
        (None,),
    ]
    # fmt: on
)
```

Using it:
```python
def test_contract_nomissing_with_missing_values(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_missing_test_table)
```

Make sure that the name of the TestTable is unique. As the test suite will fail if multiple test
tables are created with the same name.

## Parsing tests

In that same functional test file for a feature or check type, we group the functional tests together
with parsing error tests.  Example of testing a parsing error:

```python
def test_no_missing_with_threshold():
    errors_str = get_parse_errors_str(
        """
          dataset: TABLE_NAME
          columns:
            - name: one
              checks:
                - type: no_missing_values
                  must_be: 5
        """
    )

    assert "Check type 'no_missing_values' does not allow for threshold keys must_..." in errors_str
```
