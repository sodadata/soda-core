#  Copyright 2020 Soda
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging

import yaml

logger = logging.getLogger(__name__)


class YamlHelper:

    @staticmethod
    def parse_yaml(yaml_str: str, description: str = None):
        try:
            return yaml.load(yaml_str, Loader=yaml.SafeLoader)
        except Exception as e:
            logger.error(f'Parsing YAML failed: {str(e)}: ({description if description else yaml_str})xWW4')

    @staticmethod
    def validate_numeric_value(column_name, key, value):
        if value is None:
            logger.info(f'There is no value specified for {key} for column {column_name}')
        elif value is isinstance(value, int):
            logger.error(f'{column_name} could not be parsed: {key}-{value} is not of a numeric type.')
            raise Exception(f'{column_name} could not be parsed: {key}-{value} is not of a numeric type.')
        else:
            return value

    @staticmethod
    def validate_list_value(column_name, key, value):
        if value is None:
            logger.info(f'There is no value specified for {key} for column {column_name}')
        elif value is isinstance(value, list):
            logger.error(f'{column_name} could not be parsed: {key}-{value} is not of a list type.')
            raise Exception(f'{column_name} could not be parsed: {key}-{value} is not of a list type.')
        else:
            return value
