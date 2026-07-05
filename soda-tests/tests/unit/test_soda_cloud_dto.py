"""Unit tests for ProfilingConfigurationDTO and its integration into DatasetConfigurationDTO."""

import pytest
from pydantic import ValidationError
from soda_core.common.soda_cloud_dto import (
    CardinalitySamplingStrategyConfigurationDTO,
    DatasetConfigurationDTO,
    ProfilingConfigurationDTO,
    TimePartitionSamplingStrategyConfigurationDTO,
)


class TestProfilingConfigurationDTOParsing:
    def test_profiling_configuration_is_enabled_parses_via_alias(self):
        dto = DatasetConfigurationDTO.model_validate(
            {
                "datasetQualifiedName": "x",
                "profilingConfiguration": {
                    "isEnabled": True,
                    "samplingStrategyConfiguration": {
                        "type": "timePartition",
                        "unitOfTime": "days",
                        "numberOfUnits": 30,
                    },
                },
            }
        )
        assert dto.profiling_configuration is not None
        assert dto.profiling_configuration.is_enabled is True

    def test_profiling_configuration_time_partition_strategy_is_typed(self):
        dto = DatasetConfigurationDTO.model_validate(
            {
                "datasetQualifiedName": "x",
                "profilingConfiguration": {
                    "isEnabled": True,
                    "samplingStrategyConfiguration": {
                        "type": "timePartition",
                        "unitOfTime": "days",
                        "numberOfUnits": 30,
                    },
                },
            }
        )
        strategy = dto.profiling_configuration.sampling_strategy_configuration
        assert isinstance(strategy, TimePartitionSamplingStrategyConfigurationDTO)
        assert strategy.type == "timePartition"
        assert strategy.unit_of_time == "days"
        assert strategy.number_of_units == 30

    def test_profiling_configuration_disabled(self):
        dto = DatasetConfigurationDTO.model_validate(
            {
                "datasetQualifiedName": "y",
                "profilingConfiguration": {
                    "isEnabled": False,
                },
            }
        )
        assert dto.profiling_configuration is not None
        assert dto.profiling_configuration.is_enabled is False
        assert dto.profiling_configuration.sampling_strategy_configuration is None

    def test_profiling_configuration_cardinality_strategy(self):
        dto = DatasetConfigurationDTO.model_validate(
            {
                "datasetQualifiedName": "z",
                "profilingConfiguration": {
                    "isEnabled": True,
                    "samplingStrategyConfiguration": {
                        "type": "cardinality",
                        "numberOfRows": 100,
                    },
                },
            }
        )
        strategy = dto.profiling_configuration.sampling_strategy_configuration
        assert isinstance(strategy, CardinalitySamplingStrategyConfigurationDTO)
        assert strategy.type == "cardinality"
        assert strategy.number_of_rows == 100

    @pytest.mark.parametrize("unit_of_time", ["hours", "days", "weeks"])
    def test_profiling_configuration_time_partition_all_units(self, unit_of_time):
        strategy = TimePartitionSamplingStrategyConfigurationDTO.model_validate(
            {"type": "timePartition", "unitOfTime": unit_of_time, "numberOfUnits": 2}
        )
        assert strategy.unit_of_time == unit_of_time
        assert strategy.number_of_units == 2

    def test_profiling_configuration_unknown_sampling_type_is_a_validation_error(self):
        with pytest.raises(ValidationError):
            ProfilingConfigurationDTO.model_validate(
                {
                    "isEnabled": True,
                    "samplingStrategyConfiguration": {
                        "type": "somethingElse",
                        "numberOfRows": 100,
                    },
                }
            )

    def test_profiling_configuration_sampling_strategy_extra_fields_pass_through(self):
        """extra=allow on the union members: unknown Cloud fields don't break parsing."""
        strategy = CardinalitySamplingStrategyConfigurationDTO.model_validate(
            {"type": "cardinality", "numberOfRows": 100, "someUnknownFutureField": "value"}
        )
        assert strategy.number_of_rows == 100


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

    def test_profiling_configuration_extra_fields_pass_through(self):
        """extra=allow means unknown Cloud fields don't cause validation errors."""
        dto = ProfilingConfigurationDTO.model_validate(
            {
                "isEnabled": True,
                "someUnknownFutureField": "value",
            }
        )
        assert dto.is_enabled is True

    def test_profiling_configuration_all_fields_optional(self):
        """An empty dict is valid — all fields are Optional."""
        dto = ProfilingConfigurationDTO.model_validate({})
        assert dto.is_enabled is None
        assert dto.sampling_strategy_configuration is None


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
    """metricMonitoringConfiguration stays a raw list[dict] on purpose (OBSL-1007):
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
