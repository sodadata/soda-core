from typing import Optional


class HistoricDescriptor:
    def __init__(
        self,
        metric: "Metric" = None,
        change_over_time_cfg: "ChangeOverTimeCfg" = None,
        anomaly_values: Optional[int] = None,
    ):
        from soda.sodacl.change_over_time_cfg import ChangeOverTimeCfg

        self.metric = metric
        self.change_over_time_cfg: ChangeOverTimeCfg = change_over_time_cfg
        self.anomaly_values: Optional[int] = anomaly_values

    def __hash__(self):
        return hash((self.metric, self.change_over_time_cfg))

    def __eq__(self, other):
        if type(other) != HistoricDescriptor:
            return False
        if self is other:
            return True
        return (
            self.metric == other.metric
            and self.change_over_time_cfg == other.change_over_time_cfg
            and self.anomaly_values == self.anomaly_values
        )

    def to_jsonnable(self):
        jsonnable = {}
        if self.metric:
            jsonnable["metric_id"] = self.metric.identity
        if self.change_over_time_cfg:
            jsonnable["change_over_time"] = self.change_over_time_cfg.to_jsonnable()
        if self.anomaly_values is not None:
            jsonnable["anomaly_values"] = self.anomaly_values
        return jsonnable
