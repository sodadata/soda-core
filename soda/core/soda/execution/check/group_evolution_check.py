from __future__ import annotations

import re

from soda.execution.check.check import Check
from soda.execution.check_outcome import CheckOutcome
from soda.execution.metric.group_evolution_metric import GroupEvolutionMetric
from soda.execution.metric.metric import Metric
from soda.soda_cloud.historic_descriptor import HistoricChangeOverTimeDescriptor
from soda.sodacl.change_over_time_cfg import ChangeOverTimeCfg
from soda.sodacl.group_evolution_check_cfg import (
    GroupEvolutionCheckCfg,
    GroupValidations,
)

KEY_GROUPS_MEASURED = "groups measured"
KEY_GROUPS_PREVIOUS = "groups previous"


# TODO - add support for cloud diagnostics and retrieving values from cloud
class GroupEvolutionCheck(Check):
    def __init__(
        self,
        check_cfg: CheckCfg,
        data_source_scan: DataSourceScan,
        partition: Partition,
    ):
        super().__init__(
            check_cfg=check_cfg,
            data_source_scan=data_source_scan,
            partition=partition,
            column=None,
        )

        self.cloud_check_type = "generic"
        from soda.sodacl.user_defined_failed_rows_check_cfg import (
            UserDefinedFailedRowsCheckCfg,
        )

        check_cfg: UserDefinedFailedRowsCheckCfg = self.check_cfg

        metric = GroupEvolutionMetric(
            data_source_scan=self.data_source_scan,
            query=check_cfg.query,
            check=self,
            partition=partition,
        )
        metric = self.data_source_scan.resolve_metric(metric)
        self.metrics[KEY_GROUPS_MEASURED] = metric

        group_evolution_check_cfg: GroupEvolutionCheckCfg = self.check_cfg
        if group_evolution_check_cfg.has_change_validations():
            historic_descriptor = HistoricChangeOverTimeDescriptor(
                metric_identity=metric.identity, change_over_time_cfg=ChangeOverTimeCfg()
            )
            self.historic_descriptors[KEY_GROUPS_PREVIOUS] = historic_descriptor

    def evaluate(self, metrics: dict[str, Metric], historic_values: dict[str, object]):
        group_evolution_check_cfg: GroupEvolutionCheckCfg = self.check_cfg

        self.measured_groups = metrics.get(KEY_GROUPS_MEASURED).value

        previous_groups = (
            historic_values.get(KEY_GROUPS_PREVIOUS).get("measurements").get("results")[0].get("value")
            if historic_values
            and historic_values.get(KEY_GROUPS_PREVIOUS, {}).get("measurements", {}).get("results", {})
            else None
        )

        self.missing_groups = []
        self.present_groups = []

        if previous_groups:
            self.group_comparator = GroupComparator(previous_groups, self.measured_groups)
        else:
            if group_evolution_check_cfg.has_change_validations():
                warning_message = "Skipping group checks since there are no historic groups available!"
                self.logs.warning(warning_message)
                self.add_outcome_reason(outcome_type="notEnoughHistory", message=warning_message, severity="warn")
                return

        if self.has_group_violations(group_evolution_check_cfg.fail_validations):
            self.outcome = CheckOutcome.FAIL
        elif self.has_group_violations(group_evolution_check_cfg.warn_validations):
            self.outcome = CheckOutcome.WARN
        else:
            self.outcome = CheckOutcome.PASS

        # TODO : workaround for check value sent to cloud
        self.check_value = 0

    def has_group_violations(self, validations: GroupValidations) -> bool:
        if validations is None:
            return False

        measured_groups = self.measured_groups
        required_groups = set()

        if validations.required_group_names:
            required_groups.update(validations.required_group_names)

        if validations:
            for required_group_name in required_groups:
                if required_group_name not in measured_groups:
                    self.missing_groups.append(required_group_name)

        if validations.forbidden_group_names:
            for forbidden_group_name in validations.forbidden_group_names:
                regex = forbidden_group_name.replace("%", ".*").replace("*", ".*")
                forbidden_pattern = re.compile(regex)
                for group_name in measured_groups:
                    if forbidden_pattern.match(group_name):
                        self.present_groups.append(group_name)

        return (
            len(self.missing_groups) > 0
            or len(self.present_groups) > 0
            or (
                validations.is_group_addition_forbidden
                and self.group_comparator
                and len(self.group_comparator.group_additions) > 0
            )
            or (
                validations.is_group_deletion_forbidden
                and self.group_comparator
                and len(self.group_comparator.group_deletions) > 0
            )
        )


class GroupComparator:
    def __init__(self, previous_groups, measured_groups):
        self.group_additions = []
        self.group_deletions = []
        self.__compute_group_changes(previous_groups, measured_groups)

    def __compute_group_changes(self, previous_groups, measured_groups):
        for previous_group in previous_groups:
            if previous_group not in measured_groups:
                self.group_additions.append(previous_group)
        for group in measured_groups:
            if group not in previous_groups:
                self.group_additions.append(group)
