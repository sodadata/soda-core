from soda_core.common.logs import Logs
from soda_core.contracts.impl.contract_yaml import ContractYaml


def test_contract_variable_resolving(env_vars: dict, logs: Logs):
    env_vars["state"] = "polluted"

    variables = {
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

    resolved = ContractYaml._resolve_variables(variables)

    assert resolved["greeting"] == "Hello, John!"
    assert resolved["message"] == "Hello, John! Welcome to Acme Inc.."
    assert resolved["company"] == "Acme Inc."
    assert resolved["circular1"] == "This has a This has a ${var.circular1} reference. reference."
    assert resolved["circular2"] == "This has a This has a This has a ${var.circular1} reference. reference. reference."
    assert resolved["self_ref"] == "I reference I reference ${var.self_ref} myself! myself!"
    assert resolved["unknown"] == "This is an ${var.UNKNOWN} var"
    assert resolved["env_resolving"] == "The state of the environment is ${env.state}"

    assert (
        "Environment variable 'state' will not be resolved because environment "
        "variables are not supported inside contract."
    ) in logs.get_errors_str()
