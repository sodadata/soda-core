import json

from sodasql.soda_server_client.soda_server_client import SodaServerClient


class MockSodaServerClient(SodaServerClient):

    # noinspection PyMissingConstructor
    def __init__(self):
        self.requests = []
        self.commands = []
        self.host = 'MockSodaServerClient'
        self.token = 'mocktoken'
        self.file_uploads = {}

    def execute_command(self, command: dict):
        # Serializing is important as it ensures no exceptions occur during serialization
        json.dumps(command, indent=2)
        # Still we use the unserialized version to check the results as that is easier
        self.commands.append(command)
        if command['type'] == 'sodaSqlScanStart':
            return {'scanReference': 'scanref-123'}

    def execute_query(self, command: dict):
        raise RuntimeError('Not supported yet')

    def _upload_file(self, headers, temp_file):
        file_id = f'file-{str(len(self.file_uploads))}'
        data = temp_file.read().decode("utf-8")
        self.file_uploads[file_id] = {
            'headers': headers,
            'data': data
        }
        temp_file.close()
        return {'fileId': file_id}

