import requests
import json
import time
import traceback

class HeimdallCursor:
    def __init__(self, log=None):
        self.log = log or (lambda *a, **k: None)  # minimal fallback if no logger provided
        self.description = None
        self._rows = []
        self._row_index = 0

        # Hardwired config per your note
        self.endpoint = "http://127.0.0.1:9090/api/v1/job"
        self.headers = {
            "X-Heimdall-User": "jparadis@pattern.com",
            "Content-Type": "application/json",
        }

    def execute(self, sql: str):
        # traceback.print_stack(limit=None, file=None)
        payload = {
            "name": "user-analytics-query",
            "version": "1.0.0",
            "command_criteria": ["type:snowflake"],
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

    def _extract_rows(self, result_payload):
        """
        Normalizes possible Heimdall result shapes into a list-of-rows.
        Expected shapes:
          - {"rows": [[...], [...]]}
          - [[...], [...]]
          - {"result": {"rows": ...}} (defensive)
          - {"data": [[...]]} (defensive)
        """
        if isinstance(result_payload, dict):
            if "rows" in result_payload and isinstance(result_payload["rows"], list):
                return [tuple(r) for r in result_payload["rows"]]
            if "result" in result_payload and isinstance(result_payload["result"], dict):
                return self._extract_rows(result_payload["result"])
            if "data" in result_payload and isinstance(result_payload["data"], list):
                return [tuple(r) for r in result_payload["data"]]
        if isinstance(result_payload, list):
            return [tuple(r) for r in result_payload]
        # fallback: no rows
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
