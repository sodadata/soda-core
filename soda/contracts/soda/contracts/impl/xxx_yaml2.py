import logging

from ruamel.yaml import YAML, CommentedMap, CommentedSeq


class Yaml2:
    ruamel_yaml: YAML = YAML()
    ruamel_yaml.preserve_quotes = True

    @classmethod
    def parse(cls, yaml_str: str) -> object:
        ruamel_value =  cls.ruamel_yaml.load(yaml_str)
        return cls.__transform(ruamel_value)

    @classmethod
    def __transform(self, ruamel_value) -> object:
        if isinstance(ruamel_value, CommentedMap):
            return self.__transform_object(ruamel_value)
        if isinstance(ruamel_value, CommentedSeq):
            return self.__transform_list(ruamel_value)
        logging.error(f"Unsupported Ruamel YAML object type: {type(ruamel_value).__name__}\n{str(ruamel_value)}")

    @classmethod
    def __transform_object(cls, ruamel_commented_map: CommentedMap) -> dict:
        d = {}
        for k, ruamel_value in ruamel_commented_map.items():
            v = cls.__transform(ruamel_value)
            d[k] = v

    @classmethod
    def __transform_list(cls, ruamel_commented_seq: CommentedSeq) -> list:
        pass
