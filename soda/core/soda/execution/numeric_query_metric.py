from decimal import Decimal
from typing import List, Optional

from soda.execution.query_metric import QueryMetric
from soda.sodacl.format_cfg import FormatHelper


class NumericQueryMetric(QueryMetric):
    def __init__(
        self,
        data_source_scan: "DataSourceScan",
        partition: Optional["Partition"],
        column: Optional["Column"],
        metric_name: str,
        metric_args: Optional[List[object]],
        filter: Optional[str],
        aggregation: Optional[str],
        check_missing_and_valid_cfg: "MissingAndValidCfg",
        column_configurations_cfg: "ColumnConfigurationsCfg",
        check: "Check",
    ):
        from soda.sodacl.missing_and_valid_cfg import MissingAndValidCfg

        merged_missing_and_valid_cfg = MissingAndValidCfg.merge(check_missing_and_valid_cfg, column_configurations_cfg)
        other_metric_args = metric_args[1:] if isinstance(metric_args, list) and len(metric_args) > 1 else None
        super().__init__(
            data_source_scan=data_source_scan,
            partition=partition,
            column=column,
            name=metric_name,
            check=check,
            identity_parts=[
                other_metric_args,
                filter,
                merged_missing_and_valid_cfg.get_identity_parts() if merged_missing_and_valid_cfg else None,
            ],
        )

        self.metric_args: Optional[List[object]] = metric_args
        self.filter: Optional[str] = filter
        self.aggregation: Optional[str] = aggregation
        self.missing_and_valid_cfg: MissingAndValidCfg = merged_missing_and_valid_cfg

        # Implementation and other non-identity fields
        self.logs = data_source_scan.scan._logs
        self.column_name = column.column_name if column else None

    def get_sql_aggregation_expression(self) -> Optional[str]:
        data_source = self.data_source_scan.data_source

        """
        Returns an aggregation SQL expression for the given metric as a str or None if It is not an aggregation metric
        """
        if self.name in ["row_count", "missing_count", "valid_count", "invalid_count"]:
            # These are the conditional count metrics
            condition = None

            if "missing_count" == self.name:
                condition = self.build_missing_condition()

            if "valid_count" == self.name:
                condition = self.build_non_missing_and_valid_condition()

            if "invalid_count" == self.name:
                missing_condition = self.build_missing_condition()
                valid_condition = self.build_valid_condition()
                if valid_condition:
                    condition = f"NOT ({missing_condition}) AND NOT ({valid_condition})"
                else:
                    self.logs.warning("Counting invalid without valid specification does not make sense")
                    condition = f"FALSE"

            if self.filter:
                condition = f"({self.filter}) AND ({condition})" if condition else self.filter

            if condition:
                return data_source.expr_count_conditional(condition=condition)
            else:
                return data_source.expr_count_all()

        if self.aggregation:
            if self.is_missing_or_validity_configured():
                self.logs.info(
                    f"Missing and validity in {self.column.column_name} will not be applied to custom aggregation expression ({self.aggregation})"
                )
            if self.filter:
                self.logs.error(
                    f"Filter ({self.filter}) can't be applied in combination with a custom metric aggregation expression ({self.aggregation})"
                )
            return self.aggregation

        values_expression = self.column.column_name

        condition_clauses = []
        if self.is_missing_or_validity_configured():
            condition_clauses.append(self.build_non_missing_and_valid_condition())
        if self.filter:
            condition_clauses.append(self.filter)
        if condition_clauses:
            condition = " AND ".join(condition_clauses)
            values_expression = data_source.expr_conditional(condition=condition, expr=self.column_name)

        numeric_format = self.get_numeric_format()
        if numeric_format:
            values_expression = data_source.cast_text_to_number(values_expression, numeric_format)

        return data_source.get_metric_sql_aggregation_expression(self.name, self.metric_args, values_expression)

    def set_value(self, value):
        if value is None or isinstance(value, int):
            self.value = value
        elif self.name in [
            "row_count",
            "missing_count",
            "invalid_count",
            "valid_count",
            "duplicate_count",
        ]:
            self.value = int(value)
        elif isinstance(value, Decimal):
            self.value = float(value)
        else:
            self.value = value

    def ensure_query(self):
        self.partition.ensure_query_for_metric(self)

    def build_missing_condition(self) -> str:
        from soda.execution.data_source import DataSource

        column_name = self.column_name
        data_source: DataSource = self.data_source_scan.data_source

        def append_missing(missing_and_valid_cfg):
            if missing_and_valid_cfg:
                if missing_and_valid_cfg.missing_values:
                    sql_literal_missing_values = data_source.literal_list(missing_and_valid_cfg.missing_values)
                    validity_clauses.append(f"{column_name} IN {sql_literal_missing_values}")

                missing_format = missing_and_valid_cfg.missing_format
                if missing_format:
                    format_cfgs = self.table.scan._configuration.format_cfgs
                    format_regex = format_cfgs.get(missing_format)
                    if not format_regex:
                        self.logs.error(
                            f"Missing format {missing_format}",
                            location=missing_and_valid_cfg.missing_format_location,
                        )
                    else:
                        format_regex = data_source.escape_regex(format_regex)
                        validity_clauses.append(data_source.expr_regexp_like(column_name, format_regex))

                if missing_and_valid_cfg.missing_regex:
                    missing_regex = missing_and_valid_cfg.missing_regex
                    validity_clauses.append(data_source.expr_regexp_like(column_name, missing_regex))

        validity_clauses = [f"{column_name} IS NULL"]

        append_missing(self.missing_and_valid_cfg)

        return " OR ".join(validity_clauses)

    def build_valid_condition(self) -> Optional[str]:
        column_name = self.column_name
        data_source = self.data_source_scan.data_source

        def append_valid(missing_and_valid_cfg):
            if missing_and_valid_cfg:
                if missing_and_valid_cfg.valid_values:
                    valid_values_sql = data_source.literal_list(missing_and_valid_cfg.valid_values)
                    in_expr = data_source.expr_in(column_name, valid_values_sql)
                    validity_clauses.append(in_expr)

                valid_format = missing_and_valid_cfg.valid_format
                if valid_format is not None:
                    format_cfgs = self.data_source_scan.scan._configuration.format_cfgs
                    format_regex = format_cfgs.get(valid_format)
                    # TODO generate error if format does not exist!
                    if not format_regex:
                        # TODO move this to a validate step between configuration parsing and execution so that it can be validated without running
                        self.logs.error(
                            f"Valid format {valid_format} does not exist",
                            location=missing_and_valid_cfg.valid_format_location,
                        )
                    else:
                        format_regex = data_source.escape_regex(format_regex)
                        regex_like_expr = data_source.expr_regexp_like(column_name, format_regex)
                        validity_clauses.append(regex_like_expr)
                if missing_and_valid_cfg.valid_regex is not None:
                    valid_regex = missing_and_valid_cfg.valid_regex
                    regex_like_expr = data_source.expr_regexp_like(column_name, valid_regex)
                    validity_clauses.append(regex_like_expr)
                if missing_and_valid_cfg.valid_length is not None:
                    length_expr = data_source.expr_length(column_name)
                    gte_expr = f"{length_expr} = {missing_and_valid_cfg.valid_length}"
                    validity_clauses.append(gte_expr)
                if missing_and_valid_cfg.valid_min_length is not None:
                    length_expr = data_source.expr_length(column_name)
                    gte_expr = f"{length_expr} >= {missing_and_valid_cfg.valid_min_length}"
                    validity_clauses.append(gte_expr)
                if missing_and_valid_cfg.valid_max_length is not None:
                    length_expr = data_source.expr_length(column_name)
                    lte_expr = f"{length_expr} <= {missing_and_valid_cfg.valid_max_length}"
                    validity_clauses.append(lte_expr)
                if missing_and_valid_cfg.valid_min is not None:
                    gte_expr = get_boundary_expr(missing_and_valid_cfg.valid_min, ">=")
                    validity_clauses.append(gte_expr)
                if missing_and_valid_cfg.valid_max is not None:
                    lte_expr = get_boundary_expr(missing_and_valid_cfg.valid_max, "<=")
                    validity_clauses.append(lte_expr)

        def get_boundary_expr(value, comparator: str):
            valid_format = self.missing_and_valid_cfg.valid_format if self.missing_and_valid_cfg else None
            if valid_format:
                cast_expr = data_source.cast_text_to_number(column_name, valid_format)
                return f"{cast_expr} {comparator} {value}"
            else:
                return f"{column_name} {comparator} {value}"

        validity_clauses = []

        append_valid(self.missing_and_valid_cfg)

        return " AND ".join(validity_clauses) if len(validity_clauses) != 0 else None

    def build_non_missing_and_valid_condition(self):
        missing_condition = self.build_missing_condition()
        valid_condition = self.build_valid_condition()
        if valid_condition:
            return f"NOT ({missing_condition}) AND ({valid_condition})"
        else:
            return f"NOT ({missing_condition})"

    def get_numeric_format(self) -> Optional[str]:
        if self.missing_and_valid_cfg and FormatHelper.is_numeric(self.missing_and_valid_cfg.valid_format):
            return self.missing_and_valid_cfg.valid_format
        return None

    def is_missing_or_validity_configured(self) -> bool:
        return self.missing_and_valid_cfg is not None
