from __future__ import annotations


class DbtCloudConfig:
    def __init__(self, api_token: str | None, account_id: str | None):
        self.api_token = api_token
        self.account_id = account_id
