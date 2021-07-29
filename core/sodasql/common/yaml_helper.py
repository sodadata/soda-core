import logging

import yaml


class YamlHelper:

    @staticmethod
    def parse_yaml(yaml_str: str, description: str = None):
        try:
            return yaml.load(yaml_str, Loader=yaml.SafeLoader)
        except Exception as e:
            logging.error(f'Parsing YAML failed: {str(e)}: ({description if description else yaml_str})xWW4')

    @staticmethod
    def validate_numeric_value(column_name, key, value):
        if value is None:
            print("column none")
            logging.info(f'There is no {column_name} key to be validated')
        elif value is not isinstance(value, int):
            logging.error(f'{column_name} could not be parsed: {key}-{value} is not of a numeric type.')
            raise Exception(f'{column_name} could not be parsed: {key}-{value} is not of a numeric type.')
        else:
            return value


    @staticmethod
    def validate_array_value(column_name, key, value):
        if value is not None and isinstance(value, list):
            return value
        else:
            logging.error(f'{column_name} could not be parsed: {key}-{value} is not a list.')
