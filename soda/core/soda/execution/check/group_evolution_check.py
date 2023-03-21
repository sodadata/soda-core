from soda.execution.check.check import Check
from soda.execution.metric.metric import Metric


class GroupEvolutionCheck(Check):
    def __init__(
        self,
        check_cfg: "CheckCfg",
        data_source_scan: "DataSourceScan",
        partition: "Partition",
    ):
        super().__init__(
            check_cfg=check_cfg,
            data_source_scan=data_source_scan,
            partition=partition,
            column=None,
        )

        self.cloud_check_type = "generic"
        # self.measured_schema: Optional[List[Dict[str, str]]] = None
        # self.schema_missing_column_names: Optional[List[str]] = None
        # self.schema_present_column_names: Optional[List[str]] = None
        # self.schema_column_type_mismatches: Optional[Dict[str, str]] = None
        # self.schema_column_index_mismatches: Optional[Dict[str, str]] = None
        # self.schema_comparator = None
        # from soda.execution.metric.schema_metric import SchemaMetric
        #
        # schema_metric = data_source_scan.resolve_metric(
        #     SchemaMetric(
        #         data_source_scan=self.data_source_scan,
        #         partition=partition,
        #         check=self,
        #     )
        # )
        # self.metrics[KEY_SCHEMA_MEASURED] = schema_metric
        #
        # schema_check_cfg: SchemaCheckCfg = self.check_cfg
        # if schema_check_cfg.has_change_validations():
        #     historic_descriptor = HistoricChangeOverTimeDescriptor(
        #         metric_identity=schema_metric.identity, change_over_time_cfg=ChangeOverTimeCfg()
        #     )
        #     self.historic_descriptors[KEY_SCHEMA_PREVIOUS] = historic_descriptor

    def evaluate(self, metrics: dict[str, Metric], historic_values: dict[str, object]):
        pass
