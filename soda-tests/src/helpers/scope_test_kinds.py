"""Test-only check-collection kinds for scope tests.

``ScopeUnsupportedImpl`` is a kind with the default ``NoScopeSupport`` that parses its YAML with
``ContractYaml``, so ``scopes`` and ``scope`` are read exactly as a data standard reads
them. It registers on import through ``CheckCollectionImpl.__init_subclass__``.
"""

from soda_core.check_collections.base import CheckCollectionImpl, CheckCollectionResult
from soda_core.contracts.impl.contract_yaml import ContractYaml

SCOPE_UNSUPPORTED_KIND: str = "scope-test-unsupported"


class ScopeUnsupportedImpl(CheckCollectionImpl):
    kind = SCOPE_UNSUPPORTED_KIND
    wire_source = "scope-test-unsupported"
    yaml_class = ContractYaml
    result_class = CheckCollectionResult
    requires_collection_id = False
