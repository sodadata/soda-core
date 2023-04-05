from soda.contract.parser.data_contract_variable_resolver import (
    DataContractVariableResolver,
)


def test_basic_variable_resolving():
    variable_resolver = DataContractVariableResolver({"ABC": "123"})
    result = variable_resolver.resolve_variables("x ${ABC} y")
    assert result == "x 123 y"


def test_basic_variable_resolving_whitespace():
    variable_resolver = DataContractVariableResolver({"ABC": "123"})
    result = variable_resolver.resolve_variables("x ${  ABC } y")
    assert result == "x 123 y"


def test_unknown_variable_resolving_whitespace():
    variable_resolver = DataContractVariableResolver({})
    result = variable_resolver.resolve_variables("x ${  ABC } y")
    assert result == "x ${  ABC } y"


def test_multiple_variables():
    variable_resolver = DataContractVariableResolver({"ABC": "1", "XYZ": "2"})
    result = variable_resolver.resolve_variables("x ${ABC}${XYZ} y")
    assert result == "x 12 y"
