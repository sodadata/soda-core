"""Unit tests for ProfilingConfigurationDTO and its integration into DatasetConfigurationDTO."""

from soda_core.common.soda_cloud_dto import (
    DatasetConfigurationDTO,
    ProfilingConfigurationDTO,
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
                        "columnName": "ts",
                    },
                },
            }
        )
        assert dto.profiling_configuration is not None
        assert dto.profiling_configuration.is_enabled is True

    def test_profiling_configuration_sampling_strategy_is_accessible(self):
        dto = DatasetConfigurationDTO.model_validate(
            {
                "datasetQualifiedName": "x",
                "profilingConfiguration": {
                    "isEnabled": True,
                    "samplingStrategyConfiguration": {
                        "type": "timePartition",
                        "columnName": "ts",
                    },
                },
            }
        )
        assert dto.profiling_configuration.sampling_strategy_configuration is not None
        assert dto.profiling_configuration.sampling_strategy_configuration["type"] == "timePartition"
        assert dto.profiling_configuration.sampling_strategy_configuration["columnName"] == "ts"

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
                        "cardinalityThreshold": 100,
                    },
                },
            }
        )
        strategy = dto.profiling_configuration.sampling_strategy_configuration
        assert strategy["type"] == "cardinality"
        assert strategy["cardinalityThreshold"] == 100

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
