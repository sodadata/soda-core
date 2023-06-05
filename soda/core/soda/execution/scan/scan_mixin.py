from soda.__version__ import SODA_CORE_VERSION


class ScanMixin:
    def scan_start(self):
        pass

    def log_version(self):
        self._logs.info(f"Soda Core {SODA_CORE_VERSION}")

    def validate_prerequisites(self) -> bool:
        return True
