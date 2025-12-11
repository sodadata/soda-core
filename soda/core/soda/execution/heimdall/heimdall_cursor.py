import requests
import json
import time
from datetime import datetime, date
from typing import Any

class HeimdallCursor:
    def __init__(self, data_source_type: str, log=None):
        self.log = log or (lambda *a, **k: None)  # minimal fallback if no logger provided
        self.description = None
        self._rows = []
        self._row_index = 0
        self.data_source_type = data_source_type

        # Hardwired config per your note
        self.endpoint = "http://127.0.0.1:9090/api/v1/job"
        self.headers = {
            "X-Heimdall-User": "jparadis@pattern.com",
            "Content-Type": "application/json",
        }

    def execute(self, sql: str):
        self.log.info(f"Data source engine: {self.data_source_type}")
        payload = {
            "name": "user-analytics-query",
            "version": "1.0.0",
            "command_criteria": [f"type:{self.data_source_type}"],
            "cluster_criteria": ["data:prod"],
            "tags": ["dag_id:jp_test"],
            "context": {"query": sql, "return_result": True},
        }

        # Submit job
        response = requests.post(self.endpoint, json=payload, headers=self.headers)
        if response.status_code != 200:
            raise Exception(f"Heimdall job failed [{response.status_code}]: {response.text}")

        # Detect expired SSO HTML response early (matching your earlier check)
        if '<p>Sign in with your account to access Amazon Cognito</p>' in response.text:
            raise Exception("SSO cookies expired, re-authorize.")

        response_payload = response.json()

        if "status" not in response_payload or "id" not in response_payload:
            raise Exception(f"Unexpected heimdall response: {response_payload}")

        job_id = response_payload["id"]
        status = response_payload.get("status")
        self.log.info(f"Heimdall Job URL: https://heimdall.aws.pattern.com/jobs/{job_id}")

        # If immediate failure or success with embedded result
        if status == "FAILED":
            raise Exception(f"Heimdall Job failed: {response_payload.get('error')}")
        if status == "SUCCEEDED" and "result" in response_payload:
            result_payload = response_payload["result"]
            self._rows = self._extract_rows(result_payload)
            self._build_description_from_rows()
            return

        # Otherwise, poll until completion
        sleep_counter = 0
        self.log.info("Waiting for Heimdall Job to complete...")
        while True:
            status_resp = requests.get(f"{self.endpoint}/{job_id}/status", headers=self.headers)
            try:
                status_payload = status_resp.json()
            except Exception:
                status_payload = {}

            if status_resp.status_code == 200 and "status" in status_payload:
                st = status_payload.get("status")
                if st == "FAILED":
                    raise Exception(f"Heimdall Job failed: {status_payload.get('error')}")
                if st == "KILLED":
                    raise Exception("Heimdall Job was killed")
                if st == "SUCCEEDED":
                    break

            time.sleep(5)
            sleep_counter += 1
            if sleep_counter > 5:
                last_status = status_payload.get("status") if isinstance(status_payload, dict) else None
                self.log.info(f"Still running, last job status: {last_status}")
                sleep_counter = 0

        # Fetch and store results (if available)
        result_resp = requests.get(f"{self.endpoint}/{job_id}/result", headers=self.headers)
        if result_resp.status_code != 200:
            raise Exception(f"Unexpected result [{result_resp.status_code}]: {result_resp.text}")
        result_payload = result_resp.json()
        self._rows = self._extract_rows(result_payload)
        self._build_description_from_rows()

    def _convert_cell(self, value: Any, type_name: str):
        if value is None:
            return None

        t = type_name.upper()

        # DATE → Python date
        if t == "DATE":
            if isinstance(value, str):
                # "2025-12-11T00:00:00Z" → date(2025, 12, 11)
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return dt.date()
            return value

        # TIMESTAMP → Python datetime
        if "TIMESTAMP" in t:
            if isinstance(value, str):
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            return value

        # NUMBER / FLOAT / INT
        if t in ("NUMBER", "FLOAT", "DOUBLE", "INT", "INTEGER", "DECIMAL"):
            if isinstance(value, str):
                return float(value) if "." in value else int(value)
            return value

        # BOOLEAN
        if t == "BOOLEAN":
            return value in ("true", "True", True, 1)

        return value

    def _extract_rows(self, result_payload):
        """
        Extract rows AND convert values according to column metadata.
        Expected structure:
        {
            "columns": [{"name": ..., "type": ...}, ...],
            "data": [[...], [...]]
        }
        """
        # Case 1 — payload has columns + data
        if isinstance(result_payload, dict) and "columns" in result_payload and "data" in result_payload:
            columns = result_payload["columns"]
            rows = result_payload["data"]

            # Build a parsed row list
            parsed_rows = []
            for row in rows:
                parsed_row = []
                for i, value in enumerate(row):
                    col_type = columns[i].get("type", "STRING")
                    parsed_value = self._convert_cell(value, col_type)
                    parsed_row.append(parsed_value)
                parsed_rows.append(tuple(parsed_row))

            # Store metadata for cursor description
            self.description = tuple(
                (col["name"], col.get("type"), None, None, None, None, None)
                for col in columns
            )

            return parsed_rows

        # Case 2 — nested "result" dict
        if isinstance(result_payload, dict) and "result" in result_payload:
            return self._extract_rows(result_payload["result"])

        # Case 3 — legacy "rows"
        if isinstance(result_payload, dict) and "rows" in result_payload:
            raw_rows = result_payload["rows"]
            return [tuple(r) for r in raw_rows]

        # Case 4 — bare list
        if isinstance(result_payload, list):
            return [tuple(r) for r in result_payload]

        return []

    def _build_description_from_rows(self):
        if self._rows:
            col_count = len(self._rows[0])
            self.description = tuple((f"col_{i}", None, None, None, None, None, None) for i in range(col_count))
        else:
            self.description = ()

    def fetchone(self):
        if self._row_index >= len(self._rows):
            return None
        row = self._rows[self._row_index]
        self._row_index += 1
        return row

    def fetchall(self):
        # return a copy to mimic many DB cursors' behavior
        return list(self._rows)

    def fetchmany(self, size: int = None):
        """
        Soda's MemorySafeCursorFetcher expects this to exist.
        Behaves like a DB-API cursor fetchmany().
        """
        if size is None:
            size = len(self._rows)

        start = self._row_index
        end = min(start + size, len(self._rows))

        batch = self._rows[start:end]
        self._row_index = end

        return batch

    def close(self):
        # nothing to do for this minimal implementation
        pass

    def reset(self):
        # reset fetching state (mirrors some DB cursors)
        self._row_index = 0
