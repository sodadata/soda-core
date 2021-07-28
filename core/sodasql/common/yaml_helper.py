import logging

import yaml
import sys

class YamlHelper:

    @staticmethod
    def parse_yaml(yaml_str: str, description: str = None):
        try:
            return yaml.load(yaml_str, Loader=yaml.SafeLoader)
        except Exception as e:
            logging.error(f'Parsing YAML failed: {str(e)}: ({description if description else yaml_str})xWW4')

    @staticmethod
    def validate_numeric_value(column_name, value):
        if isinstance(value, int):
            logging.info("all good!")
            return value
        else:
            logging.error(f'{column_name} could not be parsed: {value} is not of a numeric type.')
            sys.exit(1)
