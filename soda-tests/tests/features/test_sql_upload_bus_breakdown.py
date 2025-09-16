import datetime
import logging

import pytest

try:
    import pandas as pd
except ImportError:
    pass
from helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.data_source_results import QueryResult
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import SodaDataTypeName, SqlDataType
from soda_core.common.sql_ast import (
    COUNT,
    CREATE_TABLE_COLUMN,
    CREATE_TABLE_IF_NOT_EXISTS,
    DROP_TABLE_IF_EXISTS,
    FROM,
    INSERT_INTO,
    LITERAL,
    SELECT,
    STAR,
    VALUES_ROW,
)
from soda_core.common.sql_dialect import SqlDialect

logger: logging.Logger = soda_logger

CSV_FILE_LOCATION = "Bus_Breakdown_and_Delays.csv"
SCHEMA_NAME = "observability_testing"  # Change to dev_observability_testing for Athena.
TABLE_NAME = "bus_breakdown"
BATCH_SIZES = {  # Depends on the database. Not all databases support very large batch inserts.
    "postgres": 10000,
    "sqlserver": 1,  # For sqlserver: batch size of 1 as it does auto conversions that fail when inserting multiple rows at once.
    "mysql": 10000,
    "bigquery": 2500,  # Bigquery limits the query size to 1024KB, so we need to use a smaller batch size. This takes a while to run!
    "snowflake": 10000,
    "oracle": 10000,
    "synapse": 1000,
    "fabric": 10000,
    "redshift": 10000,
    "athena": 500,
}

# Some datasources might prefer to use VARCHAR instead of TEXT, or the other way around. Here we can change it easily.
TEXT_TYPE_TO_USE = SodaDataTypeName.VARCHAR

# Map the columns to data types
COLUMN_TO_DATA_TYPE_MAPPING: dict[str, SodaDataTypeName] = {
    "School_Year": TEXT_TYPE_TO_USE,
    "Busbreakdown_ID": SodaDataTypeName.INTEGER,
    "Run_Type": TEXT_TYPE_TO_USE,
    "Bus_No": TEXT_TYPE_TO_USE,
    "Route_Number": TEXT_TYPE_TO_USE,
    "Reason": TEXT_TYPE_TO_USE,
    "Schools_Serviced": TEXT_TYPE_TO_USE,
    "Occurred_On": SodaDataTypeName.TIMESTAMP,
    "Created_On": SodaDataTypeName.TIMESTAMP,
    "Boro": TEXT_TYPE_TO_USE,
    "Bus_Company_Name": TEXT_TYPE_TO_USE,
    "How_Long_Delayed": TEXT_TYPE_TO_USE,
    "Number_Of_Students_On_The_Bus": SodaDataTypeName.INTEGER,
    "Has_Contractor_Notified_Schools": TEXT_TYPE_TO_USE,
    "Has_Contractor_Notified_Parents": TEXT_TYPE_TO_USE,
    "Have_You_Alerted_OPT": TEXT_TYPE_TO_USE,
    "Informed_On": SodaDataTypeName.TIMESTAMP,
    "Incident_Number": TEXT_TYPE_TO_USE,
    "Last_Updated_On": SodaDataTypeName.TIMESTAMP,
    "Breakdown_or_Running_Late": TEXT_TYPE_TO_USE,
    "School_Age_or_PreK": TEXT_TYPE_TO_USE,
}

TIMESTAMP_COLUMNS = ["Occurred_On", "Created_On", "Informed_On", "Last_Updated_On"]
INTEGER_COLUMNS = ["Busbreakdown_ID", "Number_Of_Students_On_The_Bus"]


def convert_timestamp_to_datetime(timestamp: str) -> datetime.datetime:
    # The timestamp is in the format "2021/01/01 10:00:00 AM"
    return datetime.datetime.strptime(timestamp, "%m/%d/%Y %I:%M:%S %p")


def convert_to_values_row(row) -> VALUES_ROW:
    result_list: list[LITERAL] = []

    # First we extract all the values
    all_values = {
        "School_Year": row["School_Year"],
        "Busbreakdown_ID": int(row["Busbreakdown_ID"]),
        "Run_Type": row["Run_Type"],
        "Bus_No": row["Bus_No"],
        "Route_Number": row["Route_Number"],
        "Reason": row["Reason"],
        "Schools_Serviced": row["Schools_Serviced"],
        "Occurred_On": row["Occurred_On"],
        "Created_On": row["Created_On"],
        "Boro": row["Boro"],
        "Bus_Company_Name": row["Bus_Company_Name"],
        "How_Long_Delayed": row["How_Long_Delayed"],
        "Number_Of_Students_On_The_Bus": row["Number_Of_Students_On_The_Bus"],
        "Has_Contractor_Notified_Schools": row["Has_Contractor_Notified_Schools"],
        "Has_Contractor_Notified_Parents": row["Has_Contractor_Notified_Parents"],
        "Have_You_Alerted_OPT": row["Have_You_Alerted_OPT"],
        "Informed_On": row["Informed_On"],
        "Incident_Number": row["Incident_Number"],
        "Last_Updated_On": row["Last_Updated_On"],
        "Breakdown_or_Running_Late": row["Breakdown_or_Running_Late"],
        "School_Age_or_PreK": row["School_Age_or_PreK"],
    }
    # Then we convert the values to literals (note: order must be maintained!)
    for key, value in all_values.items():
        if pd.isnull(
            value
        ):  # We need to check for NaN, as pandas will convert the empty strings to NaN, which databases cannot handle -> convert to Null
            result_list.append(LITERAL(None))
        else:
            if key in TIMESTAMP_COLUMNS:
                value = convert_timestamp_to_datetime(value)
            elif key in INTEGER_COLUMNS:
                value = int(value)
            else:
                value = str(value)
            # Make sure that the column is of the correct type, sometimes we get errors with this.
            if COLUMN_TO_DATA_TYPE_MAPPING[key] == SodaDataTypeName.TIMESTAMP:
                assert isinstance(value, datetime.datetime)
            elif COLUMN_TO_DATA_TYPE_MAPPING[key] == SodaDataTypeName.INTEGER:
                assert isinstance(value, int)
            elif COLUMN_TO_DATA_TYPE_MAPPING[key] == TEXT_TYPE_TO_USE:
                assert isinstance(value, str)
            else:
                raise ValueError(f"Unknown column type: {COLUMN_TO_DATA_TYPE_MAPPING[key]}")
            result_list.append(LITERAL(value))

    return VALUES_ROW(result_list)


@pytest.mark.skip(
    reason="This test is a hack to upload the bus breakdown dataset to the test database. It should not be considered a part of the test suite."
)
def test_insert_bus_breakdown_dataset(data_source_test_helper: DataSourceTestHelper):
    """
    This is a very hacky way to upload a dataset (specifically the bus breakdown dataset) to a database.
    Figured this is the easiest way to do this quickly, as we already have the connection, sqldialect,... for each datasource.
    You will see some hacks in this code, such as the manual setting of the dataset_prefix to the schema name, so we can use the existing test helper.
    If you have the time, feel free to refactor this :).
    Bus dataset downloaded from: https://catalog.data.gov/dataset/bus-breakdown-and-delays

    Note: this test requires to have pandas installed!
    """

    data_source_impl: DataSourceImpl = data_source_test_helper.data_source_impl
    data_source_type: str = data_source_impl.type_name
    sql_dialect: SqlDialect = data_source_impl.sql_dialect
    dataset_prefix = data_source_test_helper.dataset_prefix

    # Create the schema
    dataset_prefix[data_source_impl.sql_dialect.get_schema_prefix_index()] = SCHEMA_NAME
    data_source_test_helper.dataset_prefix = dataset_prefix
    data_source_test_helper.create_test_schema_if_not_exists()

    # Create the table
    my_table_name = TABLE_NAME
    my_table_name = sql_dialect.qualify_dataset_name(dataset_prefix, my_table_name)

    # Drop table if exists
    drop_table_sql = sql_dialect.build_drop_table_sql(DROP_TABLE_IF_EXISTS(fully_qualified_table_name=my_table_name))
    data_source_impl.execute_update(drop_table_sql)

    # Create the columns
    create_table_columns = [
        CREATE_TABLE_COLUMN(
            name="School_Year",
            type=SqlDataType(name=COLUMN_TO_DATA_TYPE_MAPPING["School_Year"], character_maximum_length=255),
            nullable=True,
        ),
        CREATE_TABLE_COLUMN(
            name="Busbreakdown_ID", type=SqlDataType(name=COLUMN_TO_DATA_TYPE_MAPPING["Busbreakdown_ID"]), nullable=True
        ),
        CREATE_TABLE_COLUMN(
            name="Run_Type",
            type=SqlDataType(name=COLUMN_TO_DATA_TYPE_MAPPING["Run_Type"], character_maximum_length=255),
            nullable=True,
        ),
        CREATE_TABLE_COLUMN(
            name="Bus_No",
            type=SqlDataType(name=COLUMN_TO_DATA_TYPE_MAPPING["Bus_No"], character_maximum_length=255),
            nullable=True,
        ),
        CREATE_TABLE_COLUMN(
            name="Route_Number",
            type=SqlDataType(name=COLUMN_TO_DATA_TYPE_MAPPING["Route_Number"], character_maximum_length=255),
            nullable=True,
        ),
        CREATE_TABLE_COLUMN(
            name="Reason",
            type=SqlDataType(name=COLUMN_TO_DATA_TYPE_MAPPING["Reason"], character_maximum_length=255),
            nullable=True,
        ),
        CREATE_TABLE_COLUMN(
            name="Schools_Serviced",
            type=SqlDataType(name=COLUMN_TO_DATA_TYPE_MAPPING["Schools_Serviced"], character_maximum_length=255),
            nullable=True,
        ),
        CREATE_TABLE_COLUMN(
            name="Occurred_On", type=SqlDataType(name=COLUMN_TO_DATA_TYPE_MAPPING["Occurred_On"]), nullable=True
        ),
        CREATE_TABLE_COLUMN(
            name="Created_On", type=SqlDataType(name=COLUMN_TO_DATA_TYPE_MAPPING["Created_On"]), nullable=True
        ),
        CREATE_TABLE_COLUMN(
            name="Boro",
            type=SqlDataType(name=COLUMN_TO_DATA_TYPE_MAPPING["Boro"], character_maximum_length=255),
            nullable=True,
        ),
        CREATE_TABLE_COLUMN(
            name="Bus_Company_Name",
            type=SqlDataType(name=COLUMN_TO_DATA_TYPE_MAPPING["Bus_Company_Name"], character_maximum_length=255),
            nullable=True,
        ),
        CREATE_TABLE_COLUMN(
            name="How_Long_Delayed",
            type=SqlDataType(name=COLUMN_TO_DATA_TYPE_MAPPING["How_Long_Delayed"], character_maximum_length=255),
            nullable=True,
        ),
        CREATE_TABLE_COLUMN(
            name="Number_Of_Students_On_The_Bus",
            type=SqlDataType(name=COLUMN_TO_DATA_TYPE_MAPPING["Number_Of_Students_On_The_Bus"]),
            nullable=True,
        ),
        CREATE_TABLE_COLUMN(
            name="Has_Contractor_Notified_Schools",
            type=SqlDataType(
                name=COLUMN_TO_DATA_TYPE_MAPPING["Has_Contractor_Notified_Schools"], character_maximum_length=255
            ),
            nullable=True,
        ),
        CREATE_TABLE_COLUMN(
            name="Has_Contractor_Notified_Parents",
            type=SqlDataType(
                name=COLUMN_TO_DATA_TYPE_MAPPING["Has_Contractor_Notified_Parents"], character_maximum_length=255
            ),
            nullable=True,
        ),
        CREATE_TABLE_COLUMN(
            name="Have_You_Alerted_OPT",
            type=SqlDataType(name=COLUMN_TO_DATA_TYPE_MAPPING["Have_You_Alerted_OPT"], character_maximum_length=255),
            nullable=True,
        ),
        CREATE_TABLE_COLUMN(
            name="Informed_On", type=SqlDataType(name=COLUMN_TO_DATA_TYPE_MAPPING["Informed_On"]), nullable=True
        ),
        CREATE_TABLE_COLUMN(
            name="Incident_Number",
            type=SqlDataType(name=COLUMN_TO_DATA_TYPE_MAPPING["Incident_Number"], character_maximum_length=255),
            nullable=True,
        ),
        CREATE_TABLE_COLUMN(
            name="Last_Updated_On", type=SqlDataType(name=COLUMN_TO_DATA_TYPE_MAPPING["Last_Updated_On"]), nullable=True
        ),
        CREATE_TABLE_COLUMN(
            name="Breakdown_or_Running_Late",
            type=SqlDataType(
                name=COLUMN_TO_DATA_TYPE_MAPPING["Breakdown_or_Running_Late"], character_maximum_length=255
            ),
            nullable=True,
        ),
        CREATE_TABLE_COLUMN(
            name="School_Age_or_PreK",
            type=SqlDataType(name=COLUMN_TO_DATA_TYPE_MAPPING["School_Age_or_PreK"], character_maximum_length=255),
            nullable=True,
        ),
    ]

    standard_columns = [column.convert_to_standard_column() for column in create_table_columns]

    # First create the table
    create_table_sql = sql_dialect.build_create_table_sql(
        CREATE_TABLE_IF_NOT_EXISTS(
            fully_qualified_table_name=my_table_name,
            columns=create_table_columns,
        )
    )
    data_source_impl.execute_update(create_table_sql)

    # Read the csv file into a pandas dataframe
    logger.info("Reading the csv file into a pandas dataframe")
    df = pd.read_csv(CSV_FILE_LOCATION, index_col=False)
    # Convert the dataframe to a list of values rows
    # We can speed this up with pandarallel if needed.
    logger.info("Converting the dataframe to a list of values rows")
    values_rows = df.apply(lambda x: convert_to_values_row(x), axis=1)
    logger.info(f"Number of values rows: {len(values_rows)}")

    # Then insert into the table
    # We do this in batches. For some databases we get errors if we do everything at once.
    batch_size = BATCH_SIZES[data_source_type]
    for i in range(0, len(values_rows), batch_size):
        batch_values_rows = values_rows[i : i + batch_size]
        insert_into_sql = sql_dialect.build_insert_into_sql(
            INSERT_INTO(
                fully_qualified_table_name=my_table_name,
                values=batch_values_rows,
                columns=standard_columns,
            )
        )
        logger.info(f"Executing the insert into sql for batch {i//batch_size + 1} of {len(values_rows)//batch_size}")
        data_source_impl.execute_update(insert_into_sql)

    # Build a select count star query to verify the rows inserted
    select_star_query = sql_dialect.build_select_sql(
        [SELECT(COUNT(STAR())), FROM(my_table_name[1:-1])]  # Remove the outer quotes, as the table will be quoted again
    )
    select_star_result: QueryResult = data_source_impl.execute_query(select_star_query)
    logger.info("Verifying that the number of rows inserted is correct")
    logger.info(f"Select star result: {select_star_result}")
    assert select_star_result.rows[0][0] == len(values_rows)
    assert select_star_result.rows[0][0] == len(df)
    logger.info("Successfully uploaded the bus breakdown dataset to the database")
