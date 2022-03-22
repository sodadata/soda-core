from typing import Optional


class StorageRef:
    def __init__(
        self,
        provider: str,
        column_count: int,
        total_row_count: int,
        stored_row_count: int,
        soda_cloud_file_id: Optional[str] = None,
        reference: Optional[str] = None,
    ):
        self.provider: str = provider
        self.column_count = column_count
        self.total_row_count: int = total_row_count
        self.stored_row_count: int = stored_row_count
        self.soda_cloud_file_id: Optional[str] = soda_cloud_file_id
        self.reference: str = reference

    def __str__(self) -> str:
        return f"{self.reference} {self.column_count}x({self.stored_row_count}/{self.total_row_count})"

    def get_cloud_diagnostics_dict(self):
        storage_ref_dict = {
            "provider": self.provider,
            "column_count": self.column_count,
            "total_row_count": self.total_row_count,
            "stored_row_count": self.stored_row_count,
        }
        if self.soda_cloud_file_id:
            storage_ref_dict["soda_cloud_file_id"] = self.soda_cloud_file_id
        if self.reference:
            storage_ref_dict["reference"] = self.reference
        return storage_ref_dict
