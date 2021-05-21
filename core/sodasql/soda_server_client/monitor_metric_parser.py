from sodasql.scan.dialect import Dialect
from sodasql.scan.parser import Parser
from sodasql.scan.scan import Scan

from sodasql.soda_server_client.monitor_metric import MonitorMetric

KEY_ID = 'id'
KEY_TYPE = 'type'
KEY_COLUMN_NAME = 'columnName'
KEY_GROUP_BY_COLUMN_NAMES = 'groupByColumnNames'
KEY_FILTER = 'filter'


class MonitorMetricParser(Parser):

    def __init__(self, monitor_metric_dict: dict, scan: Scan):
        super().__init__('Monitor SQL metric')
        self.dialect: Dialect = scan.dialect
        self._push_context(monitor_metric_dict)
        try:
            metric_type = self.get_str_required(KEY_TYPE)
            metric_id = self.get_str_required(KEY_ID)
            column_name = self.get_str_optional(KEY_COLUMN_NAME)
            group_by_column_names = self.get_list_optional(KEY_GROUP_BY_COLUMN_NAMES)

            self.monitor_metric = MonitorMetric(
                scan=scan,
                metric_id=metric_id,
                metric_type=metric_type,
                column_name=column_name,
                group_by_column_names=group_by_column_names
            )

            filter_expression_dict = self.get_dict_optional(KEY_FILTER)

            qualified_group_column_names = []
            if group_by_column_names:
                qualified_group_column_names = [self.dialect.qualify_column_name(group_field)
                                                for group_field in group_by_column_names]
            filter_condition = self.dialect.sql_expression(expression_dict=filter_expression_dict,
                                                           scan_time=scan.time)

            self.monitor_metric.build_sql(
                qualified_group_column_names,
                filter_condition,
                scan.qualified_table_name)

        finally:
            self._pop_context()
