def is_soda_library_available() -> bool:
    try:
        from soda_library.execution.check.cloud_check import CloudCheckMixin  # noqa

        return True
    except ModuleNotFoundError:
        return False
