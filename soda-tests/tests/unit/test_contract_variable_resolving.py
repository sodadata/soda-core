from soda_core.common.logs import Logs
from soda_core.contracts.impl.contract_yaml import ContractYaml


def test_contract_variable_resolving(env_vars: dict, logs: Logs):
    env_vars["state"] = "polluted"

    variable_values: dict[str, str] = {
        "now": "-- ${soda.NOW} --",
        "name": "John",
        "greeting": "Hello, ${var.name}!",
        "message": "${var.greeting} Welcome to ${var.company}.",
        "company": "Acme Inc.",
        "circular1": "This has a ${var.circular2} reference.",
        "circular2": "This has a ${var.circular1} reference.",
        "self_ref": "I reference ${var.self_ref} myself!",
        "unknown": "This is an ${var.UNKNOWN} var",
        "env_resolving": "The state of the environment is ${env.state}",
    }

    soda_variable_values: dict[str, str] = {"NOW": "soda-iso-8601-time-stamp"}

    resolved = ContractYaml._resolve_variables(
        variable_values=variable_values, soda_variable_values=soda_variable_values
    )

    assert resolved["greeting"] == "Hello, John!"
    assert resolved["message"] == "Hello, John! Welcome to Acme Inc.."
    assert resolved["company"] == "Acme Inc."
    assert resolved["circular1"] == "This has a This has a ${var.circular1} reference. reference."
    assert resolved["circular2"] == "This has a This has a This has a ${var.circular1} reference. reference. reference."
    assert resolved["self_ref"] == "I reference I reference ${var.self_ref} myself! myself!"
    assert resolved["unknown"] == "This is an ${var.UNKNOWN} var"
    assert resolved["env_resolving"] == "The state of the environment is ${env.state}"
    assert resolved["now"] == "-- soda-iso-8601-time-stamp --"

    assert (
        "Environment variable 'state' will not be resolved because environment "
        "variables are not supported inside contract."
    ) in logs.get_errors_str()


def test_reserved_soda_template_slots_pass_through_unresolved(logs: Logs):
    """`${soda.PAGINATION}` is a template slot the ENGINE fills later (per page, by
    soda-reconciliation's query mode), not a parse-time variable: the resolver must not
    error on it as an unknown soda variable, and must never substitute a value into it —
    only normalize its spacing."""
    from soda_core.common.yaml import VariableResolver

    # Embedded in a larger string (the common case: a source_query ending in the marker).
    assert (
        VariableResolver.resolve("SELECT id FROM orders\n${soda.PAGINATION}", soda_variable_values={"NOW": "ts"})
        == "SELECT id FROM orders\n${soda.PAGINATION}"
    )
    # The whole value (the fullmatch fast path), spacing normalized.
    assert VariableResolver.resolve("${ soda.PAGINATION }", soda_variable_values={"NOW": "ts"}) == "${soda.PAGINATION}"
    assert logs.get_errors_str() == ""

    # A genuinely unknown soda variable still errors — the reservation is one name, not a
    # loophole.
    VariableResolver.resolve("${soda.UNKNOWN_THING}", soda_variable_values={"NOW": "ts"})
    assert "not available in the 'soda' namespace" in logs.get_errors_str()
