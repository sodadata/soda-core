from contracts.helpers.contract_parse_errors import get_parse_errors_str


def test_duplicate_check_identity_uniqueness():
    errors_str = get_parse_errors_str(
        """
          dataset: TABLE_NAME
          columns:
            - name: one
              checks:
                - type: missing_count
                  must_be: 5
                - type: missing_count
                  must_be: 5
        """
    )

    assert "Duplicate check identity" in errors_str
