from textwrap import dedent
from typing import Optional

from soda_core.contracts.contract_verification import CheckResult


def dedent_and_strip(text: str) -> str:
    return dedent(text).strip()


def get_diagnostic_value(check_result: CheckResult, diagnostic_name: str) -> Optional[any]:
    return check_result.diagnostic_metric_values.get(diagnostic_name)
