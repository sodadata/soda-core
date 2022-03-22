from ruamel.yaml import YAML, StringIO

__yaml = YAML()
__yaml.indent(mapping=2, sequence=4, offset=2)


def to_yaml_str(yaml_object) -> str:
    if yaml_object is None:
        return ""
    stream = StringIO()
    __yaml.dump(yaml_object, stream)
    return stream.getvalue()
