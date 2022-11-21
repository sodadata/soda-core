import dask.dataframe as dd
import dask.datasets
import pandas as pd
from dask_sql import Context
from soda.scan import Scan

scan = Scan()
scan.set_scan_definition_name("test")
scan.set_data_source_name("dask")
# Execute a scan
scan.set_verbose(True)

# create a context to register tables
c = Context()
scan.add_dask_context(c)
# create a table and register it in the context
df = dask.datasets.timeseries().reset_index()
df_employee = dd.from_pandas(
    pd.DataFrame({"id": [1, 2, 3], "email": ["baturay@gmail.com", "baturay2@gmail.com", "baturay3@gmail.com"]}),
    npartitions=1,
)
df_employee = dd.from_pandas(
    pd.DataFrame({"email": ["baturay@gmail.com", "baturay2@gmail.com", "baturay3@gmail.com"]}),
    npartitions=1,
)
df["email"] = "baturay@gmail.com"
c.create_table("timeseries", df)
c.create_table("employee", df_employee)


checks = """
# for each dataset T:
#   datasets:
#     - include %
#   checks:
#     - row_count > 0
profile columns:
  columns:
    - employee.%
# automated monitoring:
#   datasets:
#     - include %
# checks for employee:
#     - values in (email) must exist in timeseries (email) # Error expected
#     - row_count same as timeseries # Error expected
#checks for timeseries:
#   - avg_x_minus_y between -1 and 1:
#       avg_x_minus_y expression: AVG(x - y)
#   - failed rows:
#       samples limit: 50
#       fail condition: x >= 3

#   - schema:
#       name: Confirm that required columns are present
#       warn:
#         when required column missing: [x]
#         when forbidden column present: [email]
#         when wrong column type:
#           email: varchar
#       fail:
#         when required column missing:
#           - y
#   - invalid_count(email) = 0:
#       valid format: email
#   - valid_count(email) > 0:
#       valid format: email
#   - duplicate_count(name) < 4:
#       samples limit: 2
#   - missing_count(y):
#       warn: when > -1
#   - missing_percent(x) < 5%
#   - missing_count(y) = 0
#   - avg(x) between -1 and 1
#   - max(x) > 0
#   - min(x) < 1:
#       filter: x > 0.2
#   - freshness(timestamp) < 1d
#   - values in (email) must exist in employee (email)
"""

scan.add_sodacl_yaml_str(checks)

scan.set_verbose(True)
scan.execute()
scan.set_verbose(True)
