import json

from sodasql.soda_server_client.soda_server_client import SodaServerClient


class MockSodaServerClient(SodaServerClient):

    # noinspection PyMissingConstructor
    def __init__(self):
        self.requests = []
        self.commands = []

    def execute_command(self, command: dict):
        json.dumps(command, indent=2)
        self.commands.append(command)
        if command['type'] == 'sodaSqlScanStart':
            return {'scanReference': 'scanref-123'}

    def execute_query(self, command: dict):
        raise RuntimeError('Not supported yet')
