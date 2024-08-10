from __future__ import annotations

import decimal
import logging
from datetime import datetime

from pyspark.sql import types, SparkSession

from contracts.helpers.contract_data_source_test_helper import ContractDataSourceTestHelper
from helpers.test_table import TestTable
from soda.contracts.impl.contract_data_source import ContractDataSource
from soda.contracts.impl.logs import Logs
from soda.contracts.impl.yaml_helper import YamlFile
from soda.data_sources.spark_df_contract_data_source import SparkDfContractDataSource
from soda.execution.data_type import DataType


class SparkDfContractDataSourceTestHelper(ContractDataSourceTestHelper):

    def __init__(self):
        self.spark_session: SparkSession = SparkSession.builder.master("local").appName("test").getOrCreate()
        super().__init__()

    def _create_database_name(self) -> str | None:
        return None

    def _create_schema_name(self) -> str | None:
        """
        In test, we register tables with createOrReplaceTempView, this is not related to any databases.
        i.e. we can't use db1.view1 to reference a local, temporary view.
        See https://stackoverflow.com/questions/45929237/how-to-specify-the-database-when-registering-a-data-frame-to-a-table-in-spark1-6
        """
        return None

    def _create_contract_data_source_yaml_dict(
        self,
        database_name: str | None,
        schema_name: str | None
    ) -> dict:
        return None

    def _create_contract_data_source(self, database_name: str | None, schema_name: str | None) -> ContractDataSource:
        data_source_yaml_dict: dict = {
            # TODO test Atlan integration
            # atlan_qualified_name: 0sd9f8s09d8f0s9d8f
        }
        logs: Logs = Logs()
        data_source_yaml_file: YamlFile = YamlFile(yaml_dict=data_source_yaml_dict, logs=logs)
        data_source = SparkDfContractDataSource(
            spark_session=self.spark_session,
            data_source_yaml_file=data_source_yaml_file
        )
        return data_source

    def create_and_insert_test_table(
        self, database_name: str | None, schema_name: str | None, test_table: TestTable
    ) -> None:
        spark_columns = []
        for test_column in test_table.test_columns:
            column_name = test_column.name
            spark_type = self.build_spark_type(test_column.data_type)
            spark_column = types.StructField(column_name, spark_type)
            spark_columns.append(spark_column)

        spark_rows = []
        if test_table.values:
            for test_row in test_table.values:
                spark_row = {}
                for i in range(0, len(spark_columns)):
                    spark_column = spark_columns[i]
                    test_column = test_table.test_columns[i]
                    test_value = test_row[i]
                    spark_value = self.convert_test_value_to_spark_value(test_value, test_column, spark_column.dataType)
                    spark_row[spark_column.name] = spark_value
                spark_rows.append(spark_row)

        spark_schema = types.StructType(spark_columns)
        spark_session = self.contract_data_source.connection.spark_session
        df = spark_session.createDataFrame(data=spark_rows, schema=spark_schema)
        logging.debug(f"Created table {test_table.unique_table_name}:")
        df.printSchema()
        df.show()
        df.createOrReplaceTempView(test_table.unique_table_name)

    @classmethod
    def build_spark_type(cls, data_type: str) -> types.DataType:
        if data_type.startswith("array[") and data_type.endswith("]"):
            element_data_type = data_type[6:-1].strip()
            element_spark_type = cls.build_spark_type(element_data_type)
            return types.ArrayType(element_spark_type)

        if data_type.startswith("struct[") and data_type.endswith("]"):
            spark_field_types = []
            field_types = data_type[7:-1].strip().split(",")
            for field_type in field_types:
                field_type_parts = field_type.split(":", 1)
                field_name = field_type_parts[0]
                field_data_type = field_type_parts[1]
                field_spark_type = cls.build_spark_type(field_data_type)
                spark_field_types.append(types.StructField(field_name, field_spark_type))
            return types.StructType(spark_field_types)

        if data_type == DataType.TEXT:
            return types.StringType()
        if data_type == DataType.INTEGER:
            return types.IntegerType()
        if data_type == DataType.DECIMAL:
            return types.DoubleType()
        if data_type == DataType.DATE:
            return types.DateType()
        if data_type == DataType.TIMESTAMP:
            return types.TimestampType()
        if data_type == DataType.TIMESTAMP_TZ:
            return types.TimestampType()
        if data_type == DataType.BOOLEAN:
            return types.BooleanType()

        if data_type == DataType.TIME:
            raise NotImplementedError(
                "Don't know how to convert time values to timestamp as Spark doesn't support times"
            )
        raise NotImplementedError(f"Test column type {data_type} not supported in spark dataframe testing")

    @classmethod
    def convert_test_value_to_spark_value(cls, test_value, test_column, sparkDataType):
        # see _acceptable_types in .venv/lib/python3.8/site-packages/pyspark/sql/types.py
        if test_value is None:
            return None
        if type(sparkDataType) in [types.FloatType, types.DoubleType]:
            return float(test_value)
        if isinstance(sparkDataType, types.DecimalType):
            return decimal.Decimal(test_value)
        if test_column.data_type == DataType.TIMESTAMP_TZ:
            return datetime.utcfromtimestamp(test_value.timestamp())
        return test_value
