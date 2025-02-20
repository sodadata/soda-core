from ruamel.yaml import YAML, StringIO


# Deprecated.  Replace all usages with YamlHelper.to_yaml below
def to_yaml_str(yaml_object) -> str:
    return YamlHelper.to_yaml(yaml_object)


class YamlHelper:
    @staticmethod
    def create_yaml() -> YAML:
        yaml = YAML()
        yaml.preserve_quotes = True
        yaml.indent(mapping=2, sequence=4, offset=2)
        return yaml

    @classmethod
    def to_yaml(cls, yaml_object) -> str:
        if yaml_object is None:
            return ""
        stream = StringIO()
        yaml = cls.create_yaml()  # Create a new YAML instance for thread safety
        yaml.dump(yaml_object, stream)
        return stream.getvalue()

    @classmethod
    def from_yaml(cls, yaml_str) -> object:
        yaml = cls.create_yaml()  # Create a new YAML instance for thread safety
        return yaml.load(yaml_str)
