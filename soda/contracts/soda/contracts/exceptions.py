from __future__ import annotations

from soda.contracts.contract import ContractResult


class SodaException(Exception):

    def __init__(self,
                 message: str | None = None,
                 contract_result: ContractResult | None = None
                 ):
        self.contract_result = contract_result
        if self.contract_result and message is None:
            message = str(self.contract_result)
        super().__init__(message)


class SodaConnectionException(SodaException):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class SodaCloudException(SodaException):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class SodaContractException(SodaException):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class ContractVerificationException(SodaException):

    def __init__(self, *args, **kwargs):
        self.contract_result = kwargs["contract_result"]
        super().__init__(self.contract_result.get_problems_text())


if __name__ == "__main__":
    raise SodaException(contract_result="ssdf")
