import json

from sodasql.soda_server_client.soda_server_client import SodaServerClient


class MockSodaServerClient(SodaServerClient):

    # noinspection PyMissingConstructor
    def __init__(self):
        self.requests = []
        self.commands = []

    def execute_command(self, command: dict):
        # Serializing is important as it ensures no exceptions occur during serialization
        json.dumps(command, indent=2)
        # Still we use the unserialized version to check the results as that is easier
        self.commands.append(command)
        if command['type'] == 'sodaSqlScanStart':
            return {'scanReference': 'scanref-123'}

    def execute_query(self, command: dict):
        raise RuntimeError('Not supported yet')
