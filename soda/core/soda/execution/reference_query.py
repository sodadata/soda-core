from soda.execution.query import Query


class ReferenceQuery(Query):
    @staticmethod
    def build_source_column_list(metric):
        source_column_names = metric.check.check_cfg.source_column_names
        return ",".join(source_column_names)

    def __init__(
        self,
        data_source_scan: "DataSourceScan",
        metric: "ReferentialIntegrityMetric",
    ):
        super().__init__(
            data_source_scan=data_source_scan,
            unqualified_query_name=f"reference[{ReferenceQuery.build_source_column_list(metric)}]",
        )

        from soda.sodacl.reference_check_cfg import ReferenceCheckCfg

        check_cfg: ReferenceCheckCfg = metric.check.check_cfg
        source_table_name = metric.partition.table.table_name
        source_column_names = check_cfg.source_column_names
        target_table_name = check_cfg.target_table_name
        target_column_names = check_cfg.target_column_names

        source_diagnostic_column_fields = "SOURCE.*"

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
        where_condition = " OR ".join(
            [f"TARGET.{target_column_name} IS NULL" for target_column_name in target_column_names]
        )

        self.sql = (
            f"SELECT {source_diagnostic_column_fields} \n"
            f"FROM {source_table_name} as SOURCE \n"
            f"     LEFT JOIN {target_table_name} as TARGET on {join_condition} \n"
            f"WHERE {where_condition}"
        )
        self.metric = metric

    def execute(self):
        self.store()
        if self.storage_ref:
            self.metric.value = self.storage_ref.total_row_count
            self.metric.invalid_references_storage_ref = self.storage_ref
