from __future__ import annotations

from soda.execution.query.query import Query


class ReferenceQuery(Query):
    @staticmethod
    def build_source_column_list(metric):
        source_column_names = metric.check.check_cfg.source_column_names
        return ",".join(source_column_names)

    def __init__(
        self,
        data_source_scan: DataSourceScan,
        metric: ReferentialIntegrityMetric,
        partition: Partition,
        samples_limit: int | None = None,
    ):
        super().__init__(
            data_source_scan=data_source_scan,
            unqualified_query_name=f"reference[{ReferenceQuery.build_source_column_list(metric)}]",
            samples_limit=samples_limit,
            partition=partition,
        )

        from soda.execution.data_source import DataSource
        from soda.sodacl.reference_check_cfg import ReferenceCheckCfg

        data_source: DataSource = data_source_scan.data_source

        check_cfg: ReferenceCheckCfg = metric.check.check_cfg
        source_table_name = data_source.qualified_table_name(metric.partition.table.table_name)
        source_column_names = check_cfg.source_column_names
        target_table_name = data_source.qualified_table_name(check_cfg.target_table_name)
        target_column_names = check_cfg.target_column_names

        selectable_source_columns = self.data_source_scan.data_source.sql_select_all_column_names(
            self.partition.table.table_name
        )
        source_diagnostic_column_fields = ", ".join([f"SOURCE.{c}" for c in selectable_source_columns])

        # TODO add global config of table diagnostic columns and apply that here
        # source_diagnostic_column_names = check_cfg.source_diagnostic_column_names
        # if source_diagnostic_column_names:
        #     source_diagnostic_column_names += source_column_names
        #     source_diagnostic_column_fields = ', '.join([f'SOURCE.{column_name}' for column_name in source_diagnostic_column_names])

        join_condition = " AND ".join(
            [
                f"SOURCE.{source_column_name} = TARGET.{target_column_names[index]}"
                for index, source_column_name in enumerate(source_column_names)
            ]
        )

        # Search for all rows where:
        # 1. source value is not null - to avoid null values triggering fails
        # 2. target value is null - this means that source value was not found in target column.
        # Passing qery is same on source side, but not null on target side.
        where_condition = " OR ".join(
            [
                f"(SOURCE.{source_column_name} IS NOT NULL AND TARGET.{target_column_name} IS NULL)"
                for source_column_name, target_column_name in zip(source_column_names, target_column_names)
            ]
        )
        passing_where_condition = " OR ".join(
            [
                f"(SOURCE.{source_column_name} IS NOT NULL AND TARGET.{target_column_name} IS NOT NULL)"
                for source_column_name, target_column_name in zip(source_column_names, target_column_names)
            ]
        )

        self.sql = self.data_source_scan.scan.jinja_resolve(
            f"SELECT {source_diagnostic_column_fields} \n"
            f"FROM {source_table_name}  SOURCE \n"
            f"     LEFT JOIN {target_table_name}  TARGET on {join_condition} \n"
            f"WHERE {where_condition}"
        )
        self.passing_sql = self.data_source_scan.scan.jinja_resolve(
            f"SELECT {source_diagnostic_column_fields} \n"
            f"FROM {source_table_name}  SOURCE \n"
            f"     LEFT JOIN {target_table_name}  TARGET on {join_condition} \n"
            f"WHERE {passing_where_condition}"
        )

        self.metric = metric

    def execute(self):
        self.store()
        if self.sample_ref:
            self.metric.set_value(self.sample_ref.total_row_count)
            if self.sample_ref.is_persisted():
                self.metric.failed_rows_sample_ref = self.sample_ref
