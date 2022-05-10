from __future__ import annotations

from typing import Optional


class StorageRef:
    def __init__(
        self,
        # Sample display name for UIs
        name: str,
        column_count: int,
        total_row_count: int,
        stored_row_count: int,
        type: str,
        reference: str,
    ):
        self.provider: str = provider
        self.column_count = column_count
        self.total_row_count: int = total_row_count
        self.stored_row_count: int = stored_row_count
        self.soda_cloud_file_id: str | None = soda_cloud_file_id
        self.reference: str = reference

    def __str__(self) -> str:
        return f"{self.reference} {self.column_count}x({self.stored_row_count}/{self.total_row_count})"

    def get_cloud_diagnostics_dict(self):
        """
        Storage ref example for soda cloud sample:
          { "name": "Failed rows",
            "columnCount": 12,
            "totalRowCount": 123877987,
            "storedRowCount": 100,
            "type": "sodaCloudFile",
            "sodaCloudFileId": "s098df0s9d8f09s8df09s"
          }

        Storage ref example for text reference:
          { "name": "Failed rows",
            "columnCount": 12,
            "totalRowCount": 123877987,
            "storedRowCount": 100,
            "type": "textReference",
            "message": "Copy this link in your browser",
            "actionLink": "https://customer.server.local/dbconsole?query=SELECT...."
          }

        Storage ref example for S3 reference:
          { "name": "Failed rows",
            "columnCount": 12,
            "totalRowCount": 123877987,
            "storedRowCount": 100,
            "type": "S3",
            "path": "s3://this/is/an/s3line"
          }
        """

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
