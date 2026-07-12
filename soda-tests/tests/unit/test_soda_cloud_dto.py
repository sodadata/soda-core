"""Unit tests for the raw-passthrough feature configs on DatasetConfigurationDTO."""

from soda_core.common.soda_cloud_dto import DatasetConfigurationDTO


class TestProfilingConfigurationPassthrough:
    """profilingConfiguration stays a raw dict on purpose:
    the soda-profiling extension (soda_profiling.profiling_config) owns the
    parsing, exactly like metricMonitoringConfiguration and
    timePartitionConfiguration."""

    BE_PROFILING_CONFIGURATION = {
        "isEnabled": True,
        "samplingStrategyConfiguration": {
            "type": "timePartition",
            "unitOfTime": "days",
            "numberOfUnits": 30,
        },
    }

    def test_profiling_configuration_parses_via_alias_as_raw_dict(self):
        dto = DatasetConfigurationDTO.model_validate(
            {
                "datasetQualifiedName": "x",
                "profilingConfiguration": self.BE_PROFILING_CONFIGURATION,
            }
        )
        assert dto.profiling_configuration == self.BE_PROFILING_CONFIGURATION
        assert isinstance(dto.profiling_configuration, dict)

    def test_profiling_configuration_round_trips_by_alias(self):
        dto = DatasetConfigurationDTO.model_validate({"profilingConfiguration": self.BE_PROFILING_CONFIGURATION})
        dumped = dto.model_dump(by_alias=True, exclude_none=True)
        assert dumped["profilingConfiguration"] == self.BE_PROFILING_CONFIGURATION

    def test_unknown_future_fields_arrive_verbatim(self):
        """soda-core does not validate profiling semantics: any dict passes through."""
        raw = {"isEnabled": True, "someUnknownFutureField": "value"}
        dto = DatasetConfigurationDTO.model_validate({"profilingConfiguration": raw})
        assert dto.profiling_configuration == raw

    def test_payloads_omitting_the_key_parse_to_none(self):
        dto = DatasetConfigurationDTO.model_validate({"datasetQualifiedName": "existing/dataset"})
        assert dto.profiling_configuration is None

    def test_empty_profiling_configuration_is_preserved_not_none(self):
        dto = DatasetConfigurationDTO.model_validate({"profilingConfiguration": {}})
        assert dto.profiling_configuration == {}


class TestTimePartitionConfigurationParsing:
    def test_time_partition_configuration_partition_column_passes_through_as_dict(self):
        dto = DatasetConfigurationDTO.model_validate(
            {
                "datasetQualifiedName": "x",
                "timePartitionConfiguration": {
                    "type": "partitionColumn",
                    "columnName": None,
                    "suggestedPartitionColumns": ["created_at"],
                },
            }
        )
        assert dto.time_partition_configuration == {
            "type": "partitionColumn",
            "columnName": None,
            "suggestedPartitionColumns": ["created_at"],
        }

    def test_time_partition_configuration_sql_expression_passes_through_as_dict(self):
        dto = DatasetConfigurationDTO.model_validate(
            {
                "datasetQualifiedName": "x",
                "timePartitionConfiguration": {"type": "sqlExpression", "sqlExpression": "ts"},
            }
        )
        assert dto.time_partition_configuration == {"type": "sqlExpression", "sqlExpression": "ts"}

    def test_time_partition_configuration_defaults_to_none(self):
        dto = DatasetConfigurationDTO.model_validate({"datasetQualifiedName": "x"})
        assert dto.time_partition_configuration is None


class TestDatasetConfigurationDTORegressionNoProfiling:
    def test_dataset_config_without_profiling_configuration_is_none(self):
        """Existing payloads that omit profilingConfiguration still parse; no regression."""
        dto = DatasetConfigurationDTO.model_validate(
            {
                "datasetQualifiedName": "existing/dataset",
                "collectFailedRows": True,
            }
        )
        assert dto.profiling_configuration is None
        assert dto.collect_failed_rows is True
        assert dto.dataset_qualified_name == "existing/dataset"

    def test_empty_dataset_config_still_parses(self):
        dto = DatasetConfigurationDTO.model_validate({})
        assert dto.profiling_configuration is None
        assert dto.collect_failed_rows is None
        assert dto.dataset_qualified_name is None

    def test_dataset_config_with_other_fields_unaffected(self):
        dto = DatasetConfigurationDTO.model_validate(
            {
                "table": "orders",
                "samplesColumns": ["id", "created_at"],
                "testRowSamplerConfiguration": {
                    "enabled": True,
                    "testRowSampler": {"type": "absoluteLimit", "limit": 50},
                },
            }
        )
        assert dto.profiling_configuration is None
        assert dto.table == "orders"
        assert dto.samples_columns == ["id", "created_at"]
        assert dto.test_row_sampler_configuration is not None
        assert dto.test_row_sampler_configuration.enabled is True


class TestMetricMonitoringConfigurationField:
    """metricMonitoringConfiguration stays a raw list[dict] on purpose:
    the metric-monitoring plugin's monitor_config owns the parsing, exactly like
    timePartitionConfiguration."""

    BE_MONITORS = [
        {"metricType": "rowCount", "metricIdentity": "e07cd407"},
        {
            "metricType": "average",
            "columnName": "cst_size",
            "metricIdentity": "ae0b66d7",
            "userConfiguration": {"validMin": 0},
        },
    ]

    def test_metric_monitoring_configuration_parses_via_alias_as_raw_dicts(self):
        dto = DatasetConfigurationDTO.model_validate(
            {
                "datasetQualifiedName": "x",
                "metricMonitoringConfiguration": self.BE_MONITORS,
            }
        )
        assert dto.metric_monitoring_configuration == self.BE_MONITORS
        assert all(isinstance(monitor, dict) for monitor in dto.metric_monitoring_configuration)

    def test_metric_monitoring_configuration_round_trips_by_alias(self):
        dto = DatasetConfigurationDTO.model_validate({"metricMonitoringConfiguration": self.BE_MONITORS})
        dumped = dto.model_dump(by_alias=True, exclude_none=True)
        assert dumped["metricMonitoringConfiguration"] == self.BE_MONITORS

    def test_payloads_omitting_the_key_parse_to_none(self):
        """Regression: existing payloads without metricMonitoringConfiguration still parse."""
        dto = DatasetConfigurationDTO.model_validate({"datasetQualifiedName": "existing/dataset"})
        assert dto.metric_monitoring_configuration is None

    def test_empty_monitor_list_is_preserved_not_none(self):
        dto = DatasetConfigurationDTO.model_validate({"metricMonitoringConfiguration": []})
        assert dto.metric_monitoring_configuration == []
