from helpers.test_table import TestTableSpecification
from soda_core.common.metadata_types import ColumnMetadata, SodaDataTypeName


def test_soda_data_type_participates_in_column_metadata_equality():
    assert ColumnMetadata(column_name="value", soda_data_type=SodaDataTypeName.INTEGER) != ColumnMetadata(
        column_name="value", soda_data_type=SodaDataTypeName.VARCHAR
    )


def test_table_specification_columns_carry_their_soda_data_type():
    specification = (
        TestTableSpecification.builder().table_purpose("column_metadata_soda_data_type").column_integer("value").build()
    )

    assert specification.columns[0].soda_data_type is SodaDataTypeName.INTEGER
