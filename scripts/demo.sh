# Copyright 2020 Soda
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

. .venv/bin/activate

# print_demodata_sql_script.py prints demodata sql script on the console
# The demodata sql script is stored as a file
python tests/demo/print_demodata_sql_script.py > ~/.soda/demodata.sql

# The demodata sql file is executed on the local postgres db
psql -h localhost  -U sodasql -d sodasql -a -f ~/.soda/demodata.sql

# Runs the scans on the demodata in postgres
python tests/demo/run_demo_scans.py
