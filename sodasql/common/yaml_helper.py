import logging

import yaml


class YamlHelper:

    @staticmethod
    def parse_yaml(yaml_str: str, description: str = None):
        try:
            return yaml.load(yaml_str, Loader=yaml.FullLoader)
        except Exception as e:
            logging.error(f'Parsing YAML failed: {str(e)}: ({description if description else yaml_str})xWW4')
