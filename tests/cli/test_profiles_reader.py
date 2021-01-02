from unittest.mock import patch

from sodasql.cli.utils.profiles_reader import ProfilesReader
from tests.cli.cli_base_test import BaseTestCase


class TestProfilesReader(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    def test_load_profiles(self):
        with patch.object(ProfilesReader, 'PROFILES_FILE_PATH', self.test_profiles_file):
            reader = ProfilesReader()
            reader.parse_logs.log()
            reader.parse_logs.assert_no_warnings_or_errors('profiles.yml')
            self.assertIsInstance(reader.profiles_yaml_dict, dict)
            self.assertTrue(reader.profiles_yaml_dict)

    def test_get_config_no_profile(self):
        with patch.object(ProfilesReader, 'PROFILES_FILE_PATH', self.test_profiles_file):
            reader = ProfilesReader()
            reader.parse_logs.log()
            reader.parse_logs.assert_no_warnings_or_errors('profiles.yml')
            self.assertIsInstance(reader.profiles_yaml_dict, dict)
            self.assertTrue(reader.profiles_yaml_dict)

    def test_get_config_none_profile(self):
        with patch.object(ProfilesReader, 'PROFILES_FILE_PATH', self.test_profiles_file):
            reader = ProfilesReader(None)
            reader.parse_logs.log()
            reader.parse_logs.assert_no_warnings_or_errors('profiles.yml')
            self.assertIsInstance(reader.profiles_yaml_dict, dict)
            self.assertTrue(reader.profiles_yaml_dict)

    def test_get_particular_config(self):
        with patch.object(ProfilesReader, 'PROFILES_FILE_PATH', self.test_profiles_file):
            reader = ProfilesReader('test')
            reader.parse_logs.log()
            reader.parse_logs.assert_no_warnings_or_errors('profiles.yml')
            self.assertIsInstance(reader.profiles_yaml_dict, dict)
            self.assertTrue(reader.profiles_yaml_dict)
