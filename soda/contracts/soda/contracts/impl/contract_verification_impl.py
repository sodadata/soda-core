from __future__ import annotations

from soda.contracts.contract import Contract, ContractResult
from soda.contracts.impl.warehouse import Warehouse
from soda.contracts.impl.yaml_helper import YamlFile


class VerificationWarehouse:
    """
    Groups all contracts for a specific warehouse. Used during contract verification execution to group all
    contracts per warehouse and ensure the warehouse is open during verification of the contract for this warehouse.
    """
    def __init__(self) -> None:
        self.warehouse: Warehouse | None = None
        self.contracts: list[Contract] = []

    def requires_with_block(self) -> bool:
        return True

    def add_contract(self, contract: Contract) -> None:
        self.contracts.append(contract)

    def ensure_open_and_verify_contracts(self) -> list[ContractResult]:
        """
        Ensures that the data source has an open connection and then invokes self.__verify_contracts()
        """
        if self.requires_with_block():
            with self.warehouse as d:
                return self.verify_contracts()
        else:
            return self.verify_contracts()

    def verify_contracts(self):
        """
        Assumes the data source has an open connection
        """
        contract_results: list[ContractResult] = []
        for contract in self.contracts:
            contract_result: ContractResult = contract.verify()
            contract_results.append(contract_result)
        return contract_results


class FileVerificationWarehouse(VerificationWarehouse):
    def __init__(self, warehouse_yaml_file: YamlFile):
        super().__init__()
        self.warehouse_file: YamlFile = warehouse_yaml_file
        self.warehouse = Warehouse.from_yaml_file(self.warehouse_file)


class SparkVerificationWarehouse(VerificationWarehouse):
    def __init__(self, spark_session: object, warehouse_name: str = "spark_ds"):
        super().__init__()
        self.spark_session: object = spark_session
        self.warehouse_name = warehouse_name
        self.warehouse = Warehouse.from_spark_session(spark_session=self.spark_session)
