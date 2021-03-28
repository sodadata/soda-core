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

import os

import yaml
from sodasql.scan.file_system import FileSystemSingleton


class EnvVars:

    # Loads the environment variables in ~/.soda/env_vars.yml under the project name key
    @classmethod
    def load_env_vars(cls, project_name: str):
        env_vars_path = f'{FileSystemSingleton.INSTANCE.user_home_dir()}/.soda/env_vars.yml'
        if FileSystemSingleton.INSTANCE.is_file(env_vars_path):
            file_contents = FileSystemSingleton.INSTANCE.file_read_as_str(env_vars_path)
            env_vars_dict = yaml.load(file_contents, Loader=yaml.SafeLoader)
            if isinstance(env_vars_dict, dict):
                project_env_vars_dict = env_vars_dict.get(project_name)
                if isinstance(project_env_vars_dict, dict):
                    for env_var_name in project_env_vars_dict:
                        env_var_value = project_env_vars_dict.get(env_var_name)
                        if isinstance(env_var_value, str):
                            os.environ[env_var_name] = env_var_value
                        elif env_var_value is None and env_var_name in os.environ:
                            del os.environ[env_var_name]
