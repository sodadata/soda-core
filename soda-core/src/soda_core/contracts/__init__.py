# Importing contract_verification_impl is a load-bearing side-effect:
# it executes the module-level ``CheckCollection.register(...)`` call that
# wires the ``contract`` kind into the CheckCollection registry. Any caller
# that imports anything from ``soda_core.contracts`` (the public facade)
# gets a fully-registered ``contract`` CheckCollection. The previous lazy
# import inside ``CheckCollectionVerificationSession.execute`` was a hidden
# coupling — anyone using the impl directly (e.g. internal tests) had to
# remember to import the impl module to trigger registration.
import soda_core.contracts.impl.contract_verification_impl  # noqa: F401
from soda_core.contracts.api import (
    publish_contract,
    test_contract,
    verify_contract,
    verify_contract_locally,
    verify_contract_on_agent,
    verify_contracts_locally,
    verify_contracts_on_agent,
)

__all__ = [
    "test_contract",
    "verify_contract",
    "verify_contract_locally",
    "verify_contract_on_agent",
    "verify_contracts_locally",
    "verify_contracts_on_agent",
    "publish_contract",
]
