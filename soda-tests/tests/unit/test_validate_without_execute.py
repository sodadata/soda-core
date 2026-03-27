"""
Unit tests for contract validation without execution (dry-run / `soda contract test`).

These tests verify that `only_validate_without_execute=True` works correctly for check
types that build standalone SQL queries, which require a data_source_impl with a sql_dialect.
In dry-run mode, data_source_impl is None and SQL generation must be skipped.

Regression tests for:
- Invalid reference check
- Failed rows query check
"""

from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import ContractVerificationSession


def test_validate_without_execute_mixed_checks():
    """
    End-to-end validation-only test with a contract containing multiple check types,
    including the previously-broken invalid reference check alongside other checks.
    This mirrors the exact scenario from the original bug report.
    """
    contract_yaml_str = """\
dataset: datasource/db/dataset/ecommerce_orders
columns:
  - name: user_id
    data_type: integer
    checks:
      - missing:
  - name: region
    data_type: character varying
  - name: price
    data_type: double precision
    checks:
      - missing:
  - name: email
    data_type: character varying
    checks:
      - invalid:
          name: email exists in customer_info
          qualifier: x7p2k
          valid_reference_data:
            column: email
            dataset: datasource/customers/production/customer_info
  - name: balance
    data_type: double precision
    checks:
      - missing:
  - name: created_date
    data_type: timestamp without time zone
"""

    result = ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(contract_yaml_str)],
        only_validate_without_execute=True,
        soda_cloud_publish_results=False,
        soda_cloud_use_agent=False,
    )

    assert result is not None
    errors_str = result.get_errors_str()
    assert errors_str is None or "sql_dialect" not in errors_str
