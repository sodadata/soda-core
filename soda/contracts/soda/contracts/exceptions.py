from __future__ import annotations


class SodaException(Exception):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class SodaConnectionException(SodaException):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class SodaCloudException(SodaException):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class ContractVerificationException(SodaException):

    def __init__(self, *args, **kwargs):
        self.contract_result = kwargs["contract_result"]
        super().__init__(self.contract_result.get_problems_text())
