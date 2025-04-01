class ContractIdentifier:
    def __init__(self, data_source: str, prefixes: list[str], dataset: str):
        self.data_source = data_source
        self.prefixes = prefixes
        self.dataset = dataset

    @classmethod
    def parse(cls, identifier: str) -> "ContractIdentifier":
        parts = identifier.split("/")
        if len(parts) < 2:
            raise ValueError("Identifier must contain at least a data source and a dataset")

        data_source = parts[0]
        dataset = parts[-1]
        prefixes = parts[1:-1] if len(parts) > 2 else []

        return cls(data_source, prefixes, dataset)

    def to_string(self) -> str:
        return "/".join([self.data_source] + self.prefixes + [self.dataset])

    def __repr__(self):
        return (f"ContractIdentifier(data_source='{self.data_source}', "
                f"prefixes={self.prefixes}, dataset='{self.dataset}')")
