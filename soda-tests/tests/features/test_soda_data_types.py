from helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.common.metadata_types import SodaDataTypeName


def test_soda_data_types(data_source_test_helper: DataSourceTestHelper):
    dialect_soda_data_type_map: dict = (
        data_source_test_helper.data_source_impl.sql_dialect.get_sql_data_type_name_by_soda_data_type_names()
    )
    unmapped_data_types: list[str] = [
        str(soda_data_type) for soda_data_type in SodaDataTypeName if soda_data_type not in dialect_soda_data_type_map
    ]
    assert unmapped_data_types == []
