#  Copyright 2021 Soda
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
from typing import Dict, Optional
import uuid
import yaml

from sodasql.scan.file_system import FileSystemSingleton

logger = logging.getLogger(__name__)

class ConfigHelper:
    DEFAULT_CONFIG = {
        'skip_telemetry': False,
        'user_cookie_id': str(uuid.uuid4())
    }
    LOAD_PATHS = ["~/.soda/config.yml", ".soda/config.yml"]
    __instance = None
    __config: Dict = None
    file_system = FileSystemSingleton.INSTANCE

    @staticmethod
    def get_instance(path: Optional[str] = None):
        if ConfigHelper.__instance == None:
            ConfigHelper()
        return ConfigHelper.__instance

    def __init__(self, path: Optional[str] = None):
        if ConfigHelper.__instance != None:
            raise Exception("This class is a singleton!")
        else:
            ConfigHelper.__instance = self

        if path:
            self.LOAD_PATHS.insert(0, path)

        self.__config = self.config

        if not self.__config:
            self.init_config_file()

        self.__ensure_basic_config()

    @property
    def config_path(self) -> str:
        return self.LOAD_PATHS[0]

    @property
    def config(self) -> Dict:
        if not self.__config:
            self.reload_config()

        return self.__config

    def reload_config(self) -> Dict:
        for path in self.LOAD_PATHS:
            logger.info(f"Trying to load Soda Config file {path}.")

            if self.file_system.file_exists(path):
                self.__config = yaml.load(
                    self.file_system.file_read_as_str(path),
                    Loader=yaml.SafeLoader
                )
                break

    def get_value(self, key: str):
        return self.config.get(key, None)

    def init_config_file(self) -> None:
        destination = self.config_path

        if self.file_system.file_exists(destination):
            logger.info(f"Config file {destination} already exists")
        else:
            logger.info(f"Creating config YAML file {destination} ...")
            self.file_system.mkdirs(self.file_system.dirname(destination))
            self.upsert_config_file(self.DEFAULT_CONFIG)

    def upsert_value(self, key: str, value: str):
        config = self.config
        config[key] = value
        self.upsert_config_file(config)
        self.reload_config()

    def upsert_config_file(self, config: Dict):
        self.file_system.file_write_from_str(
            self.config_path,
            yaml.dump(
                config,
                default_flow_style=False,
                sort_keys=False
            )
        )

    def generate_user_cookie_id(self) -> str:
        return str(uuid.uuid4())

    def __ensure_basic_config(self) -> None:
        for key, value in self.DEFAULT_CONFIG.items():
            if key not in self.config:
                self.upsert_value(key, value)

    @property
    def skip_telemetry(self) -> bool:
        return self.config.get("skip_telemetry", False)