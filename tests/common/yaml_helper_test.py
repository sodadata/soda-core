import unittest
from sodasql.common.yaml_helper import YamlHelper


class YamlHelperTest(unittest.TestCase):
    def test_valid_numeric_value(self):
        value = YamlHelper.validate_numeric_value("column_name", "key", 2)
        self.assertEqual(value, 2)

    def test_valid_array_value(self):
        value = YamlHelper.validate_list_value("column_name", "key", [1, 2, 3])
        self.assertEqual(value, [1, 2, 3])

    def test_invalid_numeric_value(self):
        value = YamlHelper.validate_numeric_value("column_name", "key", None)
        self.assertEqual(value, None)

    def test_invalid_array_value(self):
        value = YamlHelper.validate_list_value("column_name", "key", None)
        self.assertEqual(value, None)



if __name__ == '__main__':
    unittest.main()
