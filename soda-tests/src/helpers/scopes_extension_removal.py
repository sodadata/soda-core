"""Keeps a test on core alone when soda-scopes is installed.

soda-scopes registers its extension on ``ContractImpl`` under ``SCOPES_EXTENSION_NAME`` when ``soda_core``
loads its plugins, and from then on every contract activates the scopes it declares. soda-extensions CI
installs soda-scopes and runs core's integration tests, so a test that pins what core does without an
extension that runs scopes opts into ``without_scopes_extension``::

    from helpers.scopes_extension_removal import without_scopes_extension  # noqa: F401

    pytestmark = pytest.mark.usefixtures("without_scopes_extension")
"""

from __future__ import annotations

import pytest
from soda_core.contracts.impl.contract_verification_impl import ContractImpl

SCOPES_EXTENSION_NAME: str = "scopes"


@pytest.fixture
def without_scopes_extension(monkeypatch: pytest.MonkeyPatch) -> None:
    """Removes the soda-scopes extension from ``ContractImpl`` for one test and puts it back after."""
    monkeypatch.delitem(ContractImpl.impl_extensions, SCOPES_EXTENSION_NAME, raising=False)
