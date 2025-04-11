from soda_core.common.data_source_impl import DataSourceImpl


def load_postgres_impl():
    from soda_postgres.common.data_sources.postgres_data_source import (
        PostgresDataSourceImpl,
    )
    from soda_postgres.model.data_source.postgres_data_source import PostgresDataSource

    PostgresDataSourceImpl.model_class = staticmethod(lambda: PostgresDataSource)
    return PostgresDataSourceImpl


DataSourceImpl.register("postgres", load_postgres_impl)
