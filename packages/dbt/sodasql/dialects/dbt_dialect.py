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

import re
import os
from datetime import date

from sodasql.scan.dialect import DBT, KEY_WAREHOUSE_TYPE, Dialect, ALL_WAREHOUSE_TYPES
from sodasql.scan.parser import Parser
from sodasql.common.yaml_helper import YamlHelper
from sodasql.scan.warehouse_yml_parser import KEY_CONNECTION


class DBTDialect(Dialect):

    def __new__(self, parser: Parser):
        if parser:
            dbt_project_dir = parser.get_str_required_env('dbt_project_dir')
            dbt_profile_filename = os.path.expanduser(parser.get_str_optional_env('dbt_profile_filename', '~/.dbt/profiles.yml'))
            dbt_project_filename = os.path.expanduser(os.path.join(dbt_project_dir, 'dbt_project.yml'))

            if os.path.isfile(dbt_project_filename) and os.path.isfile(dbt_profile_filename):
                with open(dbt_project_filename, 'r') as fp:
                    dbt_project = YamlHelper.parse_yaml(fp.read(), dbt_project_filename)

                with open(dbt_profile_filename, 'r') as fp:
                    dbt_profiles = YamlHelper.parse_yaml(fp.read(), dbt_profile_filename)

                profile_name = dbt_project['profile']
                if profile_name in dbt_profiles:
                    dbt_target = dbt_profiles[profile_name]['target']
                    dbt_profile = dbt_profiles[profile_name]['outputs'][dbt_target]

                    if dbt_profile['type'] in ALL_WAREHOUSE_TYPES:
                        dbt_profile['username'] = dbt_profile['user']
                        dbt_profile['password'] = dbt_profile['pass']
                        dbt_profile['database'] = dbt_profile['dbname']

                        p = Parser(description=dbt_profile_filename)
                        p._push_context(object=dbt_profile, name=KEY_CONNECTION)
                        return Dialect.create(p)
                else:
                    raise RuntimeError(f"Could not file profile '{ profile_name }' in '{ dbt_profile_filename }'.")
            else:
                raise RuntimeError(f"Either dbt_project_filename '{ dbt_project_filename}', or dbt_profile_filename '{dbt_profile_filename}' does not exist.")
