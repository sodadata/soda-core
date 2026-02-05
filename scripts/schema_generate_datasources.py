from typing import Dict
from pydantic import TypeAdapter
from soda_core.model.data_source.data_source import DataSourceBase

import json

# Todo: import automagically using entry points
from soda_postgres.model.data_source.postgres_data_source import PostgresDataSource
from soda_postgres.model.data_source.postgres_connection_properties import *

from soda_snowflake.model.data_source.snowflake_data_source import SnowflakeDataSource
from soda_snowflake.model.data_source.snowflake_connection_properties import *

from soda_databricks.model.data_source.databricks_data_source import DatabricksDataSource
from soda_databricks.model.data_source.databricks_connection_properties import *


# def get_all_model_subclasses(base_class):
#     # Recursively get all subclasses (even nested ones)
#     subclasses = set()

#     def recurse(cls):
#         for sub in cls.__subclasses__():
#             subclasses.add(sub)
#             recurse(sub)

#     recurse(base_class)
#     return list(subclasses)


# def generate_all_schemas(base_class) -> Dict[str, dict]:
#     models = get_all_model_subclasses(base_class)
#     definitions = get_all_model_subclasses
#     return {
#         model.__name__: model.model_json_schema(by_alias=True, ref_template="#/definitions/{model}") for model in models
#     }


# openapi_schema = {
#     "title": "Soda Datasource Data Models",
#     "type": "object",
#     "definitions": generate_all_schemas(DataSourceBase),
# }
# print(json.dumps(openapi_schema, indent=2))

# with open("data_source_schema.json", "w") as f:
#     json.dump(openapi_schema, f, indent=4)

adapter = TypeAdapter(DataSourceBase)
schema = adapter.json_schema()
print(json.dumps(schema, indent=2))
