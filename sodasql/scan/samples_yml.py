from dataclasses import dataclass


@dataclass
class SamplesYml:

    dataset_limit: int
    dataset_tablesample: str
    failed_limit: int
    failed_tablesample: str
    passed_limit: int
    passed_tablesample: int

    def with_defaults(self, default_samples_yml):
        return SamplesYml(
           dataset_limit=self.dataset_limit if self.dataset_limit is not None else default_samples_yml.dataset_limit,
           dataset_tablesample=self.dataset_tablesample if self.dataset_tablesample is not None else default_samples_yml.dataset_tablesample,
           failed_limit=self.failed_limit if self.failed_limit is not None else default_samples_yml.failed_limit,
           failed_tablesample=self.failed_tablesample if self.failed_tablesample is not None else default_samples_yml.failed_tablesample,
           passed_limit=self.passed_limit if self.passed_limit is not None else default_samples_yml.passed_limit,
           passed_tablesample=self.passed_tablesample if self.passed_tablesample is not None else default_samples_yml.passed_tablesample,
        )

    def is_failed_rows_enabled(self):
        return self.failed_limit or self.failed_tablesample

    def is_passing_rows_enabled(self):
        return self.passed_limit or self.passed_tablesample
