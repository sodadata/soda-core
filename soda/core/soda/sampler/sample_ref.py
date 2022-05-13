from __future__ import annotations


class SampleRef:
    def __init__(
        self,
        # Sample display name for UIs
        name: str,
        column_count: int,
        total_row_count: int,
        stored_row_count: int,
        type: str,
        soda_cloud_file_id: str | None = None,
        message: str | None = None,
        link: str | None = None,
    ):
        self.name: str = name
        self.column_count = column_count
        self.total_row_count: int = total_row_count
        self.stored_row_count: int = stored_row_count
        self.type: str = type
        self.soda_cloud_file_id: str | None = soda_cloud_file_id
        self.message: str | None = message
        self.link: str | None = link

    def __str__(self) -> str:
        sample_dimension = f"{self.column_count}x({self.stored_row_count}/{self.total_row_count})"
        return " ".join(
            [
                e
                for e in [self.type, self.soda_cloud_file_id, self.message, self.link, sample_dimension]
                if e is not None
            ]
        )

    def get_cloud_diagnostics_dict(self):
        sample_ref_dict = {
            "type": self.type,
            "column_count": self.column_count,
            "total_row_count": self.total_row_count,
            "stored_row_count": self.stored_row_count,
        }
        if self.soda_cloud_file_id:
            sample_ref_dict["soda_cloud_file_id"] = self.soda_cloud_file_id
        if self.message:
            sample_ref_dict["message"] = self.message
        if self.link:
            sample_ref_dict["link"] = self.link
        return sample_ref_dict
