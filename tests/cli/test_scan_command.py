from tests.cli.cli_base_test import BaseTestCase


class TestScan(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._create_test_table(["name VARCHAR(255)",
                                "size INTEGER"],
                               ["('one',    1)",
                                "('two',    2)",
                                "('three',  3)",
                                "(null,     null)"])

    def test_scan_command(self):
        result = self._execute_cli(['scan',
                                   self._get_test_resource_path('test_project'),
                                   self.test_warehouse,
                                   self.test_table])
        self.assertEqual(result.exit_code, 0)
