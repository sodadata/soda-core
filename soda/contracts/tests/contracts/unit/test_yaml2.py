from soda.contracts.impl.yaml2 import Yaml2


def test_yaml2():
    c = Yaml2.parse("""
        dataset: lskdjfslkd
        columns:
        - name: id
          checks:
          - type: no_missing
    """)
    assert c["columns"][1].line == 3
