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
from tests.local.sql.validity.test_number_validity_formats import TestNumberValidityFormats
from tests.local.sql.validity.test_network_validity_formats import TestNetworkValidityFormats
from tests.local.sql.validity.test_date_and_time_validity_formats import TestDateAndTimeValidityFormats
from tests.local.sql.validity.test_user_info_validity_formats import TestPersonalInfoValidityFormats


class ValidityTestSuite(
    TestNumberValidityFormats,
    TestNetworkValidityFormats,
    TestDateAndTimeValidityFormats,
    TestPersonalInfoValidityFormats
):
    pass
