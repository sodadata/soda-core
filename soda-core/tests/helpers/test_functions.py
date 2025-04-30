from textwrap import dedent
from typing import Optional

from soda_core.contracts.contract_verification import CheckResult, NumericDiagnostic


def dedent_and_strip(text: str) -> str:
    return dedent(text).strip()


def get_diagnostic_value(check_result: CheckResult, diagnostic_name: str) -> Optional[any]:
    return next(
        (d.value for d in check_result.diagnostics if isinstance(d, NumericDiagnostic) and d.name == diagnostic_name),
        None,
    )
