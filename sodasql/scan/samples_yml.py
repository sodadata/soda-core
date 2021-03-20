from dataclasses import dataclass


@dataclass
class SamplesYml:

    table_limit: int
    table_tablesample: str
    failed_limit: int
    failed_tablesample: str
    passed_limit: int
    passed_tablesample: int

    def with_defaults(self, default_samples_yml):
        return SamplesYml(
           table_limit=self.table_limit if self.table_limit is not None else default_samples_yml.table_limit,
           table_tablesample=self.table_tablesample if self.table_tablesample is not None else default_samples_yml.table_tablesample,
           failed_limit=self.failed_limit if self.failed_limit is not None else default_samples_yml.failed_limit,
           failed_tablesample=self.failed_tablesample if self.failed_tablesample is not None else default_samples_yml.failed_tablesample,
           passed_limit=self.passed_limit if self.passed_limit is not None else default_samples_yml.passed_limit,
           passed_tablesample=self.passed_tablesample if self.passed_tablesample is not None else default_samples_yml.passed_tablesample,
        )

    def is_failed_enabled(self):
        return self.failed_limit or self.failed_tablesample

    def is_passed_enabled(self):
        return self.passed_limit or self.passed_tablesample

    def is_table_enabled(self):
        return self.table_limit is not None or self.table_tablesample is not None
