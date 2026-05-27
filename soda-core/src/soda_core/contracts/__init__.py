from soda_core.contracts.api import (
    publish_contract,
    test_contract,
    verify_contract,
    verify_contract_locally,
    verify_contract_on_agent,
    verify_contract_on_runner,
    verify_contracts_locally,
    verify_contracts_on_agent,
    verify_contracts_on_runner,
)

__all__ = [
    "test_contract",
    "verify_contract",
    "verify_contract_locally",
    "verify_contract_on_runner",
    "verify_contracts_locally",
    "verify_contracts_on_runner",
    # Deprecated aliases kept for backwards compatibility:
    "verify_contract_on_agent",
    "verify_contracts_on_agent",
    "publish_contract",
]
