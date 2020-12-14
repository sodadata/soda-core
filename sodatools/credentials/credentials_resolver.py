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


class CredentialsResolver:

    @classmethod
    def resolve(cls, configuration_dict: dict, credentials_key: str):
        value = configuration_dict.get(credentials_key)
        if value:
            return value

        env_var = configuration_dict.get(credentials_key+'_env_var')
        if env_var:
            value = os.getenv(env_var)
            if not value:
                raise RuntimeError(f'Environment variable {env_var} not defined')
            return value

        # TODO add support for parameter store credentials resolving

        return None
