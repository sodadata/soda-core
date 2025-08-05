from __future__ import annotations

from typing import Optional, Type
from pydantic import BaseModel
from enum import Enum

from soda_core.common.soda_cloud import SodaCloud

class IRequestStatus(str, Enum):
    pass

class IContractRequest(BaseModel):
    pass

class IContractProposal(BaseModel):
    pass

class IContractRequestExtension:
    _registry: dict[str, Type[IContractRequestExtension]] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        fully_qualified_name = cls.__module__ + "." + cls.__name__
        cls._registry[fully_qualified_name] = cls

    @classmethod
    def instance(cls, identifier: Optional[str] = None) -> IContractRequestExtension:
        if not cls._registry:
            raise NotImplementedError(f"No contract request extension implementations registered.")
        if not identifier:
            identifier = next(iter(cls._registry))
        impl_cls = cls._registry[identifier]
        return impl_cls()

    def fetch_contract_proposal(
        self,
        soda_cloud: Optional[SodaCloud],
        request_number: int,
        proposal_number: Optional[int] = None,
    ) -> IContractProposal:
        raise NotImplementedError("No implementation for fetch_contract_proposal() yet. Please install an appropriate plugin.")
    
    def push_contract_proposal(
        self,
        soda_cloud: SodaCloud,
        request_number: int,
        contents: str,
        comment: Optional[str] = None,
    ) -> IContractProposal:
        raise NotImplementedError("No implementation for push_contract_proposal() yet. Please install an appropriate plugin.")
    
    def transition_contract_request(
        self,
        soda_cloud: SodaCloud,
        request_number: int,
        status: IRequestStatus,
    ) -> IContractRequest:
        raise NotImplementedError("No implementation for transition_contract_request() yet. Please install an appropriate plugin.")