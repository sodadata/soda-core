def is_soda_library_available() -> bool:
    try:
        from soda.execution.check.cloud_check import CloudCheckMixin

        return True
    except ModuleNotFoundError:
        return False
