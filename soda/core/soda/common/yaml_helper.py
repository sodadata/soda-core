import threading

from ruamel.yaml import YAML, StringIO


def create_yaml() -> YAML:
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=4, offset=2)
    return yaml


# Deprecated.  Replace all usages with YamlHelper.to_yaml below
def to_yaml_str(yaml_object) -> str:
    return YamlHelper.to_yaml(yaml_object)


class YamlHelper:
    """
    A helper class to serialize and deserialize objects to and from YAML format.
    This class is thread-safe and ensures that each thread has its own instance of the YAML parser/dumper.

    Usage:
    YamlHelper.to_yaml(yaml_object)
    YamlHelper.from_yaml(yaml_string)
    """

    _thread_local = threading.local()

    @classmethod
    def _get_yaml(cls):
        """
        Returns a thread-local YAML instance for serializing and deserializing YAML data.
        If no instance exists for the current thread, a new one is created.

        :return: a YAML instance specific to the current thread.
        """
        if not hasattr(cls._thread_local, "yaml"):
            cls._thread_local.yaml = create_yaml()
        return cls._thread_local.yaml

    @classmethod
    def to_yaml(cls, yaml_object) -> str:
        """
        Serializes a Python object into a YAML formatted string.

        :param object yaml_object: The Python object to serialize.
        :return: the YAML formatted string, or an empty string if the input is None.
        """
        if yaml_object is None:
            return ""
        with StringIO() as stream:
            yaml_instance = cls._get_yaml()
            yaml_instance.dump(yaml_object, stream)
            return stream.getvalue()

    @classmethod
    def from_yaml(cls, yaml_str) -> object:
        """
        Deserializes a YAML formatted string into a Python object.

        :param str yaml_str: the YAML formatted string to deserialize.
        :return: the deserialized Python object, or None if the input is None or empty.
        """
        if yaml_str is None or not yaml_str.strip():
            return None
        yaml_instance = cls._get_yaml()
        return yaml_instance.load(yaml_str)
