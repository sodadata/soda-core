from soda.contract.parser.data_contract_variable_resolver import DataContractVariableResolver


def test_basic_variable_resolving():
<<<<<<< HEAD
    variable_resolver = ParserResolver({"ABC": "123"})
=======
    variable_resolver = DataContractVariableResolver({'ABC': '123'})
>>>>>>> 54dd0f18 (Contracts cleanup)
    result = variable_resolver.resolve_variables("x ${ABC} y")
    assert result == "x 123 y"


def test_basic_variable_resolving_whitespace():
<<<<<<< HEAD
    variable_resolver = ParserResolver({"ABC": "123"})
=======
    variable_resolver = DataContractVariableResolver({'ABC': '123'})
>>>>>>> 54dd0f18 (Contracts cleanup)
    result = variable_resolver.resolve_variables("x ${  ABC } y")
    assert result == "x 123 y"


def test_unknown_variable_resolving_whitespace():
    variable_resolver = DataContractVariableResolver({})
    result = variable_resolver.resolve_variables("x ${  ABC } y")
    assert result == "x ${  ABC } y"


def test_multiple_variables():
<<<<<<< HEAD
    variable_resolver = ParserResolver({"ABC": "1", "XYZ": "2"})
=======
    variable_resolver = DataContractVariableResolver({
        "ABC": "1",
        "XYZ": "2"
    })
>>>>>>> 54dd0f18 (Contracts cleanup)
    result = variable_resolver.resolve_variables("x ${ABC}${XYZ} y")
    assert result == "x 12 y"
