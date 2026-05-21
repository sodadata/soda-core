"""Lock per-check identity disambiguation across check-collection subtypes.

Two non-contract collections with identical-shaped checks on the same
dataset must produce distinct identity hashes — backend's ``tests`` table
is keyed off identity, so collisions silently overwrite history.

Contract identities are byte-identical to today's emission (preserves
Cloud history) via ``ContractImpl.identity_prefix() == ()``. Non-contract
subtypes inherit the base default which mixes ``(wire_source,
collection_id)`` into the per-check hash.
"""

from __future__ import annotations

from soda_core.contracts.impl.contract_verification_impl import CheckImpl, ContractImpl


class _StubDataSourceImpl:
    name = "test_ds"


class _ContractStub:
    """Sentinel that mirrors the surface ``CheckImpl._build_identity`` reads
    AND the ``ContractImpl.identity_prefix()`` override (returns ``()`` so
    contract identities stay byte-identical to historical emissions).
    """

    wire_source = "soda-contract"
    collection_id = None
    data_source_impl = _StubDataSourceImpl()
    dataset_prefix = "schema"
    dataset_name = "table"

    # Borrow the production override verbatim so any future refactor that
    # changes ContractImpl's identity prefix is exercised by these tests.
    identity_prefix = ContractImpl.identity_prefix


class _DataStandardStub:
    """Non-contract sentinel — overrides ``identity_prefix`` to mix
    ``(wire_source, collection_id)`` into the per-check identity hash.
    """

    wire_source = "data-standard"
    data_source_impl = _StubDataSourceImpl()
    dataset_prefix = "schema"
    dataset_name = "table"

    def __init__(self, collection_id: str):
        self.collection_id = collection_id

    def identity_prefix(self) -> tuple:
        return (self.wire_source, self.collection_id)


def test_contract_identity_prefix_is_empty_tuple():
    """``ContractImpl`` overrides ``identity_prefix`` to return ``()`` so
    its per-check identity hash stays byte-identical to every prior
    contract verification. The base default (``(wire_source,
    collection_id)``) would mutate every existing contract's identity.
    """
    assert "identity_prefix" in ContractImpl.__dict__
    contract_stub = ContractImpl.__new__(ContractImpl)
    assert contract_stub.identity_prefix() == ()


def test_contract_identity_byte_identical_to_no_prefix_mixin():
    """For contracts, ``_build_identity`` must produce the same hash as if
    no prefix mix-in existed. The prefix loop iterates an empty tuple →
    zero bytes added before the existing identity fields.
    """
    hash_with_prefix_mechanism = CheckImpl._build_identity(
        contract_impl=_ContractStub(),
        column_impl=None,
        check_type="row_count",
        qualifier=None,
        extra_identity_properties=None,
    )

    # Re-compute manually with the original (pre-prefix) algorithm.
    from soda_core.common.consistent_hash_builder import ConsistentHashBuilder

    expected = ConsistentHashBuilder(8)
    expected.add_property("dso", "test_ds")
    expected.add_property("pr", "schema")
    expected.add_property("ds", "table")
    expected.add_property("c", None)
    expected.add_property("t", "row_count")
    expected.add_property("q", None)
    assert hash_with_prefix_mechanism == expected.get_hash()


def test_two_data_standards_same_check_shape_different_collection_ids_produce_different_identities():
    """Two data standards with identical-shaped checks on the same dataset
    must produce different identity hashes — otherwise backend's ``tests``
    table silently overwrites rows.
    """
    standard_a = _DataStandardStub(collection_id="standard_a")
    standard_b = _DataStandardStub(collection_id="standard_b")

    hash_a = CheckImpl._build_identity(
        contract_impl=standard_a,
        column_impl=None,
        check_type="row_count",
        qualifier=None,
    )
    hash_b = CheckImpl._build_identity(
        contract_impl=standard_b,
        column_impl=None,
        check_type="row_count",
        qualifier=None,
    )

    assert hash_a != hash_b, "Two data standards with same check-shape must disambiguate by collection_id"


def test_data_standard_identity_differs_from_contract_for_same_check_shape():
    """Same dataset + check shape, different wire_source → distinct
    identity. This catches the case where a future ``DataStandardImpl``
    accidentally inherits the empty default and collides with a
    pre-existing contract check on the same dataset.
    """
    contract = _ContractStub()
    standard = _DataStandardStub(collection_id="standard_a")

    contract_hash = CheckImpl._build_identity(
        contract_impl=contract,
        column_impl=None,
        check_type="row_count",
        qualifier=None,
    )
    standard_hash = CheckImpl._build_identity(
        contract_impl=standard,
        column_impl=None,
        check_type="row_count",
        qualifier=None,
    )
    assert contract_hash != standard_hash


def test_data_standards_with_same_collection_id_produce_identical_identities():
    """Sanity-check the mixing is deterministic — two stubs with the same
    ``(wire_source, collection_id)`` produce the same identity for the
    same check shape. Without this, re-running the same data standard
    would produce a new identity on every scan.
    """
    standard_one = _DataStandardStub(collection_id="standard_a")
    standard_two = _DataStandardStub(collection_id="standard_a")

    hash_one = CheckImpl._build_identity(
        contract_impl=standard_one,
        column_impl=None,
        check_type="missing",
        qualifier=None,
    )
    hash_two = CheckImpl._build_identity(
        contract_impl=standard_two,
        column_impl=None,
        check_type="missing",
        qualifier=None,
    )
    assert hash_one == hash_two
