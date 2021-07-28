import unittest
from sodasql.common.yaml_helper import YamlHelper


class YamlHelperTest(unittest.TestCase):
    def test_valid_numeric_value(self):
        value = YamlHelper.validate_numeric_value("column_name", 2)
        self.assertEqual(value, 2)


if __name__ == '__main__':
    unittest.main()
