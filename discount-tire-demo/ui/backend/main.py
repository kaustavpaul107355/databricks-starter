import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel


class GenieQuery(BaseModel):
    question: str


class GenieResponse(BaseModel):
    summary: str
    sql: Optional[str] = None


BASE_DIR = Path(__file__).resolve().parents[1]
DIST_DIR = BASE_DIR / "dist"

app = FastAPI()

if DIST_DIR.exists():
    app.mount("/assets", StaticFiles(directory=DIST_DIR / "assets"), name="assets")


def _get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise HTTPException(status_code=500, detail=f"Missing required env var: {name}")
    return value


def _extract_summary(message: Dict[str, Any]) -> str:
    attachments = message.get("attachments", [])
    for attachment in attachments:
        text_content = attachment.get("text", {}).get("content")
        if text_content:
            return text_content.strip()
    return "Genie returned a response, but no summary text was found."


def _find_sql_payload(payload: Any) -> Optional[str]:
    if isinstance(payload, dict):
        for _, value in payload.items():
            sql_text = _find_sql_payload(value)
            if sql_text:
                return sql_text
    elif isinstance(payload, list):
        for item in payload:
            sql_text = _find_sql_payload(item)
            if sql_text:
                return sql_text
    elif isinstance(payload, str):
        if "select" in payload.lower():
            return payload
    return None


def _extract_sql(message: Dict[str, Any], query_result: Optional[Dict[str, Any]]) -> Optional[str]:
    candidate = message.get("query") or message.get("sql")
    if isinstance(candidate, str):
        return candidate

    if query_result:
        statement = query_result.get("statement_response", {}).get("statement", {})
        sql_text = statement.get("query")
        if isinstance(sql_text, str):
            return sql_text

    return _find_sql_payload(message) or _find_sql_payload(query_result)


def _poll_genie_message(
    base_url: str, conversation_id: str, message_id: str, headers: Dict[str, str]
) -> Dict[str, Any]:
    timeout_seconds = 180
    poll_interval = 1.5
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        response = requests.get(
            f"{base_url}/conversations/{conversation_id}/messages/{message_id}",
            headers=headers,
            timeout=30,
        )
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.text)

        payload = response.json()
        status = payload.get("status")
        if status in {"COMPLETED", "FAILED"}:
            return payload
        time.sleep(poll_interval)

    raise HTTPException(status_code=504, detail="Genie query timed out.")


@app.post("/api/genie/query", response_model=GenieResponse)
def run_genie_query(request: GenieQuery) -> GenieResponse:
    if not request.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty.")

    host = _get_env("DATABRICKS_HOST")
    token = _get_env("DATABRICKS_TOKEN_FOR_GENIE")
    space_id = _get_env("GENIE_SPACE_ID")

    base_url = f"https://{host}/api/2.0/genie/spaces/{space_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    start_response = requests.post(
        f"{base_url}/start-conversation",
        json={"content": request.question},
        headers=headers,
        timeout=30,
    )
    if start_response.status_code != 200:
        raise HTTPException(status_code=start_response.status_code, detail=start_response.text)

    start_payload = start_response.json()
    conversation_id = start_payload.get("conversation_id")
    message_id = start_payload.get("message_id")
    if not conversation_id or not message_id:
        raise HTTPException(status_code=500, detail="Invalid response from Genie.")

    message_payload = _poll_genie_message(base_url, conversation_id, message_id, headers)
    if message_payload.get("status") != "COMPLETED":
        raise HTTPException(status_code=500, detail="Genie query failed.")

    result_response = requests.get(
        f"{base_url}/conversations/{conversation_id}/messages/{message_id}/query-result",
        headers=headers,
        timeout=30,
    )
    query_result = result_response.json() if result_response.status_code == 200 else None

    summary = _extract_summary(message_payload)
    sql_text = _extract_sql(message_payload, query_result)

    return GenieResponse(summary=summary, sql=sql_text)


@app.get("/")
def root() -> FileResponse:
    if not DIST_DIR.exists():
        raise HTTPException(status_code=500, detail="dist/ folder not found.")
    return FileResponse(DIST_DIR / "index.html")


@app.get("/{path:path}")
def spa_fallback(path: str) -> FileResponse:
    if not DIST_DIR.exists():
        raise HTTPException(status_code=500, detail="dist/ folder not found.")
    return FileResponse(DIST_DIR / "index.html")
