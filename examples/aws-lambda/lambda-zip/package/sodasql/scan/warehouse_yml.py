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
from dataclasses import dataclass
from typing import Optional

from sodasql.scan.dialect import Dialect
from sodasql.soda_server_client.soda_server_client import SodaServerClient


@dataclass
class WarehouseYml:

    dialect: Dialect = None
    name: str = None
    soda_host: Optional[str] = None
    soda_port: Optional[int] = None
    soda_protocol: Optional[str] = None
    soda_api_key_id: Optional[str] = None
    soda_api_key_secret: Optional[str] = None
