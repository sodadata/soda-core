from __future__ import annotations

DBT_CLOUD_FALLBACK_ACCESS_URL = "cloud.getdbt.com"


class DbtCloudConfig:
    def __init__(
        self,
        api_token: str | None,
        account_id: str | None,
        access_url: str | None = DBT_CLOUD_FALLBACK_ACCESS_URL,
    ):
        self.api_token = api_token
        self.account_id = account_id
        self.access_url = access_url
        self.api_url = f"https://{self.access_url}/api/v2/accounts/"
