import logging

from soda.common.logs import Logs as SodaClLogs

from soda.contracts.impl.logs import Log, LogLevel, Logs

logger = logging.getLogger("soda.contracts.sodacl_log_converter")


class SodaClLogConverter(SodaClLogs):

    def __init__(self, logs: Logs):
        self.logs: Logs = logs

    def log_into_buffer(self, level, message, location, doc, exception):
        self.log(level=level, message=message, location=location, doc=doc, exception=exception)

    def log(self, level, message, location, doc, exception):
        log_level: LogLevel = LogLevel[level.name]
        self.logs._log(Log(log_level, message, location, exception))
