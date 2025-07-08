from typing import Any


def assert_dict(actual: dict, expected: dict):
    _assert_recursive_equals(path=[], actual=actual, expected=expected)


def _assert_recursive_equals(path: list[str], actual: Any, expected: Any):
    if isinstance(expected, dict) and isinstance(actual, dict):
        for key, value in expected.items():
            _assert_recursive_equals(path=path + [key], actual=actual.get(key), expected=value)
    elif isinstance(expected, list) and isinstance(actual, list):
        for index, element in enumerate(expected):
            superset_value = actual[index] if len(actual) > index else None
            _assert_recursive_equals(path=path + [str(index)], actual=superset_value, expected=element)
    else:
        assert expected == actual, f"{'.'.join(path)} expected {expected}, but was {actual}"
