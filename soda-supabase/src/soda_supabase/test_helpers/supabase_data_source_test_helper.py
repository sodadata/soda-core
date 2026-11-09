from __future__ import annotations

import os

from soda_postgres.test_helpers.postgres_data_source_test_helper import PostgresDataSourceTestHelper


class SupabaseDataSourceTestHelper(PostgresDataSourceTestHelper):
    def _create_database_name(self) -> str | None:
        return os.getenv("SUPABASE_DATABASE", "soda_test")

    def _create_data_source_yaml_str(self) -> str:
        # sslmode falls back to prefer rather than the type's require default, so the suite also runs
        # against a local Postgres without SSL. Set SUPABASE_SSLMODE=require for a real Supabase project.
        return f"""
            type: supabase
            name: {self.name}
            connection:
                host: {os.getenv("SUPABASE_HOST", "localhost")}
                user: {os.getenv("SUPABASE_USERNAME", "soda_test")}
                password: {os.getenv("SUPABASE_PASSWORD")}
                port: {int(os.getenv("SUPABASE_PORT", "5432"))}
                database: {self.dataset_prefix[0]}
                sslmode: {os.getenv("SUPABASE_SSLMODE", "prefer")}
        """
