from __future__ import annotations

from soda_core.plugins import Plugin


class CoreCheckTypesPlugin(Plugin):
    @classmethod
    def load(cls) -> CoreCheckTypesPlugin:
        cls.register_check_types()

        return cls()

    @classmethod
    def register_check_types(cls) -> None:
        from soda_core.contracts.impl.check_types.schema_check_yaml import (
            SchemaCheckYamlParser,
        )
        from soda_core.contracts.impl.contract_verification_impl import (
            CheckImpl,
            CheckYaml,
        )

        CheckYaml.register(SchemaCheckYamlParser())
        from soda_core.contracts.impl.check_types.schema_check import SchemaCheckParser

        CheckImpl.register(SchemaCheckParser())

        from soda_core.contracts.impl.check_types.missing_check_yaml import (
            MissingCheckYamlParser,
        )

        CheckYaml.register(MissingCheckYamlParser())
        from soda_core.contracts.impl.check_types.missing_check import (
            MissingCheckParser,
        )

        CheckImpl.register(MissingCheckParser())

        from soda_core.contracts.impl.check_types.invalidity_check_yaml import (
            InvalidCheckYamlParser,
        )

        CheckYaml.register(InvalidCheckYamlParser())
        from soda_core.contracts.impl.check_types.invalidity_check import (
            InvalidCheckParser,
        )

        CheckImpl.register(InvalidCheckParser())

        from soda_core.contracts.impl.check_types.duplicate_check_yaml import (
            DuplicateCheckYamlParser,
        )

        CheckYaml.register(DuplicateCheckYamlParser())
        from soda_core.contracts.impl.check_types.duplicate_check import (
            DuplicateCheckParser,
        )

        CheckImpl.register(DuplicateCheckParser())

        from soda_core.contracts.impl.check_types.row_count_check_yaml import (
            RowCountCheckYamlParser,
        )

        CheckYaml.register(RowCountCheckYamlParser())
        from soda_core.contracts.impl.check_types.row_count_check import (
            RowCountCheckParser,
        )

        CheckImpl.register(RowCountCheckParser())

        from soda_core.contracts.impl.check_types.aggregate_check import (
            AggregateCheckParser,
        )

        CheckImpl.register(AggregateCheckParser())

        from soda_core.contracts.impl.check_types.aggregate_check_yaml import (
            AggregateCheckYamlParser,
        )

        CheckYaml.register(AggregateCheckYamlParser())

        from soda_core.contracts.impl.check_types.metric_check import MetricCheckParser

        CheckImpl.register(MetricCheckParser())

        from soda_core.contracts.impl.check_types.metric_check_yaml import (
            MetricCheckYamlParser,
        )

        CheckYaml.register(MetricCheckYamlParser())

        from soda_core.contracts.impl.check_types.freshness_check_yaml import (
            FreshnessCheckYamlParser,
        )

        CheckYaml.register(FreshnessCheckYamlParser())
        from soda_core.contracts.impl.check_types.freshness_check import (
            FreshnessCheckParser,
        )

        CheckImpl.register(FreshnessCheckParser())

        from soda_core.contracts.impl.check_types.failed_rows_check_yaml import (
            FailedRowsCheckYamlParser,
        )

        CheckYaml.register(FailedRowsCheckYamlParser())
        from soda_core.contracts.impl.check_types.failed_rows_check import (
            FailedRowsCheckParser,
        )

        CheckImpl.register(FailedRowsCheckParser())
