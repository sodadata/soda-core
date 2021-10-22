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
from pathlib import Path
import yaml
import logging


class CLIHelper:

    @staticmethod
    def set_yaml_value(first_run_value):
        home_folder = str(Path.home())
        file_name = f"{home_folder}/.soda/config.yml"
        with open(file_name) as f:
            doc = yaml.safe_load(f)
        doc['first_run'] = first_run_value
        with open(file_name, 'w') as f:
            yaml.safe_dump(doc, f, default_flow_style=False)

    @staticmethod
    def welcome_message():
        logger = logging.getLogger(__name__)
        home_folder = str(Path.home())
        config_file = Path(f"{home_folder}/.soda/config.yml")
        with open(config_file, "r") as s:
            try:
                first_run_value = yaml.safe_load(s)["first_run"]
                if first_run_value == "yes":
                    logger.info("""   _____________________________________________________
                            / Welcome to Soda SQL! \n                              \
                            | If you have any questions, consider joining\n        |
                            | our Slack Community https://community.soda.io/slack\n|
                            | You can report any issues you found on our Github \n |
                            / https://github.com/sodadata/soda-sql                 \
                             -----------------------------------------------------""")
                    CLIHelper.set_yaml_value("no")
                else:
                    pass
            except Exception as e:
                logger.error(e)


