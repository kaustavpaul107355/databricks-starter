"""
Genie Output Validation Script

This script validates that Genie responses are correctly parsed by the server's
extract_summary function. It's used for testing and quality assurance.

Usage:
    export DATABRICKS_HOST=your-workspace.cloud.databricks.com
    export DATABRICKS_TOKEN_FOR_GENIE=your-token
    export GENIE_SPACE_ID=your-space-id
    python backend/validate_genie_outputs.py

The script will run a set of test questions through Genie and verify that
summaries are extracted correctly.
"""
import os
import sys
import time
from typing import Dict, Any
from urllib.request import Request, urlopen

import json

import server


def api_request(url: str, method: str, payload: Dict[str, Any], headers: Dict[str, str]) -> Dict[str, Any]:
    data = json.dumps(payload).encode("utf-8") if payload else None
    request = Request(url, data=data, headers=headers, method=method)
    with urlopen(request, timeout=30) as response:
        body = response.read().decode("utf-8")
        return json.loads(body) if body else {}


def run_validation(questions):
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN_FOR_GENIE")
    space_id = os.getenv("GENIE_SPACE_ID")
    if not host or not token or not space_id:
        raise RuntimeError("Missing DATABRICKS_HOST, DATABRICKS_TOKEN_FOR_GENIE, or GENIE_SPACE_ID.")

    base_url = f"https://{host}/api/2.0/genie/spaces/{space_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    failures = 0
    for question in questions:
        start_payload = api_request(f"{base_url}/start-conversation", "POST", {"content": question}, headers)
        conversation_id = start_payload.get("conversation_id")
        message_id = start_payload.get("message_id")
        if not conversation_id or not message_id:
            print(f"[FAIL] {question}: Genie start-conversation failed")
            failures += 1
            continue

        message_payload = None
        for _ in range(40):
            message_payload = api_request(
                f"{base_url}/conversations/{conversation_id}/messages/{message_id}",
                "GET",
                None,
                headers,
            )
            if message_payload.get("status") in {"COMPLETED", "FAILED"}:
                break
            time.sleep(2)

        if not message_payload or message_payload.get("status") != "COMPLETED":
            print(f"[FAIL] {question}: Genie message failed")
            failures += 1
            continue

        query_result = api_request(
            f"{base_url}/conversations/{conversation_id}/messages/{message_id}/query-result",
            "GET",
            None,
            headers,
        )

        summary, source = server.extract_summary(message_payload, query_result, question=question)
        print(f"[OK] {question}: summary_source={source} summary={summary}")

    return failures


if __name__ == "__main__":
    questions = [
        "revenue for last quarter",
        "sales for last 7 days",
        "revenue growth last quarter",
    ]
    failures = run_validation(questions)
    sys.exit(1 if failures else 0)
