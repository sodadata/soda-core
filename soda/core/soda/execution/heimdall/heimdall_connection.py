from soda.execution.heimdall.heimdall_cursor import HeimdallCursor


class HeimdallConnection:
    def __init__(self, log=None):
        self.log = log

    def cursor(self):
        return HeimdallCursor(log=self.log)

    def close(self):
        pass
