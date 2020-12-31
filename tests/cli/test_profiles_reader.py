from unittest import TestCase
import os
import logging
from unittest.mock import patch

from sodasql.cli.utils.profiles_reader import ProfilesReader
from .cli_base_test import BaseTestCase


class TestProfilesReader(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.expected_profiles_dict = {'default': {'target': 'postgres-dev',
                                                  'outputs': {'postgres-dev': {'type': 'postgres',
                                                                               'host': 'localhost',
                                                                               'username': 'sodasql',
                                                                               'password': 'sodasql',
                                                                               'database': 'sodasql',
                                                                               'schema': 'public'}}},
                                      'test': {'target': 'snowflake-test',
                                               'outputs': {'snowflake-test': {'type': 'snowflake',
                                                                              'username': '<Snowflake Username>',
                                                                              'password': '<Snowflake Password>',
                                                                              'account': '<Snowflake Account>',
                                                                              'warehouse': '<Snowflake Warehouse>',
                                                                              'database': '<Snowflake Database>',
                                                                              'schema': '<Snowflake Schema>'}}}}

    def test_load_profiles(self):
        with patch.object(ProfilesReader, 'PROFILES_FILE_PATH',
                          self.test_profiles_file):
            reader = ProfilesReader()
            self._assert_logs(reader.parse_logs)
            self.assertDictEqual(reader.profiles_yaml_dict,
                                 self.expected_profiles_dict)

    def test_get_config_no_profile(self):
        with patch.object(ProfilesReader, 'PROFILES_FILE_PATH',
                          self.test_profiles_file):
            reader = ProfilesReader()
            self._assert_logs(reader.parse_logs)
            self.assertDictEqual(reader.configuration,
                                 self.expected_profiles_dict['default']['outputs']['postgres-dev'])

    def test_get_config_none_profile(self):
        with patch.object(ProfilesReader, 'PROFILES_FILE_PATH',
                          self.test_profiles_file):
            reader = ProfilesReader(None)
            self._assert_logs(reader.parse_logs)
            self.assertDictEqual(reader.configuration,
                                 self.expected_profiles_dict['default']['outputs']['postgres-dev'])

    def test_get_particular_config(self):
        with patch.object(ProfilesReader, 'PROFILES_FILE_PATH',
                          self.test_profiles_file):
            reader = ProfilesReader('test')
            self._assert_logs(reader.parse_logs)
            self.assertDictEqual(reader.configuration,
                                 self.expected_profiles_dict['test']['outputs']['snowflake-test'])
