from soda.execution.heimdall.heimdall_cursor import HeimdallCursor


class HeimdallConnection:
    def __init__(self, data_source_type, log=None):
        self.log = log
        self.data_source_type = data_source_type

    def cursor(self):
        return HeimdallCursor(data_source_type=self.data_source_type, log=self.log)

    def close(self):
        pass
