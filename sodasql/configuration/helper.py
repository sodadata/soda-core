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

# with open(f'cfg/{sql_store_name}/{table_name}/scan.yml') as f:
#     scan_dict = yaml.load(f, Loader=yaml.FullLoader)
#     scan_dict['table_name'] = table_name
#     scan_configuration = ScanConfiguration(scan_dict)


# with open(f'cfg/{sql_store_name}/connection.yaml') as f:
#     connection_dict = yaml.load(f, Loader=yaml.FullLoader)
#     logging.debug(str(connection_dict))
#     connection_dict['name'] = sql_store_name
#     sql_store = SqlStore.create(connection_dict)
from sodasql.scan.parse_logs import ParseLogs


def parse_int(cfg_dict: dict, key: str, parse_logs: ParseLogs, cfg_description: str, default_value: int = None):
    if cfg_dict is None:
        return None
    value = cfg_dict.get(key)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except Exception:
            parse_logs.error(
                f'Invalid configuration.  Expected integer in {cfg_description}.{key}, but was: {str(value)}')
            return None
    if value is None:
        return default_value
    parse_logs.error(f'Invalid configuration.  Expected integer in {cfg_description}.{key}, but was type {str(type(value))}: {str(value)}')
