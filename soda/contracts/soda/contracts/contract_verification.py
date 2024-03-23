from __future__ import annotations

from dataclasses import dataclass

from soda.contracts.contract import ContractResult
from soda.contracts.impl.logs import Logs
from soda.contracts.soda_cloud import SodaCloud


class ContractVerification:

    def __init__(self):
        self.contract_yaml_strs: list[ContractYamlStr] = []
        self.contract_yaml_files: list[str] = []
        self.contract_dict: dict | None = None
        self.variables: dict[str, str] = {}
        self.soda_cloud: SodaCloud | None = None
        self.logs: Logs | None = None

    @classmethod
    def from_contract_yaml_str(self, contract_yaml_str: str, file_path: str | None = None) -> ContractVerification:
        self.contract_yaml_strs.append(ContractYamlStr(contract_yaml_str=contract_yaml_str, file_path=file_path))
        return self

    def with_contract_yaml_file(self, file_path: str) -> ContractVerification:
        self.contract_yaml_file_paths.append(file_path)

    def execute(self) -> ContractResult:
        try:
            with open(file_path) as f:
                contract_yaml_str = f.read()
                self.with_yaml_str(files_path=files_path, contract_yaml_str=contract_yaml_str)
        except OSError as e:
            self.logs.error(f"Could not read file {files_path}: {e}", exception=e)
        return self

        pass


@dataclass
class ContractYamlStr:
    contract_yaml_str: str
    file_path: str | None = None
