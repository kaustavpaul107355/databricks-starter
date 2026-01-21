import gzip
import json
import logging
import mimetypes
import os
import re
import ssl
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional, List
from datetime import datetime
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import threading

try:
    import databricks.sql as dbsql
except Exception:  # pragma: no cover
    dbsql = None

# Initialize logger before any usage
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
logger = logging.getLogger("discount_tire_demo")

try:
    from backend.db_pool import run_sql_with_pool, get_sql_pool
    _USE_POOL = True
except ImportError:
    _USE_POOL = False
    logger.warning("Connection pool not available, falling back to direct connections")


# Configuration constants
BASE_DIR = Path(__file__).resolve().parents[1]
DIST_DIR = BASE_DIR / "dist"
GENIE_CACHE_TTL_SECONDS = int(os.getenv("GENIE_CACHE_TTL_SECONDS", "300"))
SQL_CACHE_TTL_SECONDS = int(os.getenv("SQL_CACHE_TTL_SECONDS", "300"))
DASHBOARD_CACHE_TTL_SECONDS = int(os.getenv("DASHBOARD_CACHE_TTL_SECONDS", "120"))
GENIE_MAX_CONCURRENT = int(os.getenv("GENIE_MAX_CONCURRENT", "1"))

# Cache stores
_GENIE_CACHE: Dict[str, Dict[str, Any]] = {}
_GENIE_CACHE_LOCK = threading.Lock()
_SQL_CACHE: Dict[str, Dict[str, Any]] = {}
_SQL_CACHE_LOCK = threading.Lock()
_DASHBOARD_CACHE: Dict[str, Dict[str, Any]] = {}
_DASHBOARD_CACHE_LOCK = threading.Lock()
_GENIE_SEMAPHORE = threading.Semaphore(GENIE_MAX_CONCURRENT)


def api_request(url: str, method: str, payload: Optional[Dict[str, Any]], headers: Dict[str, str]) -> tuple[int, Dict[str, Any]]:
    data = json.dumps(payload).encode("utf-8") if payload else None
    request = Request(url, data=data, headers=headers, method=method)
    insecure = os.getenv("DATABRICKS_INSECURE", "").strip().lower() in {"1", "true", "yes"}
    try:
        if insecure:
            context = ssl._create_unverified_context()
            response = urlopen(request, context=context, timeout=30)
        else:
            response = urlopen(request, timeout=30)
        with response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body) if body else {}
    except HTTPError as exc:
        body = exc.read().decode("utf-8") if exc.fp else ""
        try:
            payload = json.loads(body) if body else {}
        except json.JSONDecodeError:
            payload = {}
        return exc.code, payload


def parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def table_to_dicts(table: Optional[Dict[str, Any]]) -> List[Dict[str, Optional[str]]]:
    if not table:
        return []
    columns = table.get("columns") or []
    rows = table.get("rows") or []
    return [
        {columns[idx]: row[idx] if idx < len(row) else None for idx in range(len(columns))}
        for row in rows
    ]


def table_first_value(table: Optional[Dict[str, Any]], column: str) -> Optional[str]:
    if not table:
        return None
    columns = table.get("columns") or []
    rows = table.get("rows") or []
    if not rows:
        return None
    try:
        idx = columns.index(column)
    except ValueError:
        return None
    return rows[0][idx] if idx < len(rows[0]) else None


def format_month_label(date_str: Optional[str]) -> Optional[str]:
    if not date_str:
        return None
    try:
        cleaned = date_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        return dt.strftime("%b %Y")
    except ValueError:
        return None


PREFERRED_TEXT_KEYS = (
    "summary",
    "answer",
    "response",
    "assistant_message",
    "content",
    "text",
    "message",
    "markdown",
)
SKIP_TEXT_KEYS = {"sql", "query", "statement", "status", "suggested_questions", "questions"}
UUID_RE = re.compile(r"^[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}$", re.IGNORECASE)
STATUS_VALUES = {"completed", "failed", "pending", "in_progress", "running"}


def is_probably_sql(text: str) -> bool:
    lowered = text.strip().lower()
    if not lowered:
        return False
    if lowered.startswith("select"):
        return True
    return "select" in lowered and "from" in lowered and ("where" in lowered or "group by" in lowered)


def is_probably_metric_name(text: str) -> bool:
    trimmed = text.strip()
    if "_" in trimmed and " " not in trimmed:
        return True
    return trimmed.isidentifier()


def collect_texts(payload: Any, parent_key: str = "") -> list[tuple[str, str]]:
    results: list[tuple[str, str]] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key in SKIP_TEXT_KEYS:
                continue
            if isinstance(value, str):
                results.append((key, value))
            elif isinstance(value, (dict, list)):
                results.extend(collect_texts(value, key))
    elif isinstance(payload, list):
        for item in payload:
            results.extend(collect_texts(item, parent_key))
    return results


def pick_best_text(
    candidates: list[tuple[str, str]],
    question: Optional[str] = None,
    blocked_values: Optional[set[str]] = None,
) -> Optional[str]:
    def score(item: tuple[str, str]) -> tuple[int, int]:
        key, text = item
        key_index = PREFERRED_TEXT_KEYS.index(key) if key in PREFERRED_TEXT_KEYS else len(PREFERRED_TEXT_KEYS)
        sql_penalty = 1 if is_probably_sql(text) else 0
        return (sql_penalty, key_index)

    normalized_question = question.strip().lower() if question else None
    blocked_values = {value.strip().lower() for value in (blocked_values or set()) if value}
    for _, text in sorted(candidates, key=score):
        cleaned = text.strip()
        if not cleaned or is_probably_sql(cleaned):
            continue
        if UUID_RE.match(cleaned):
            continue
        if cleaned.lower() in STATUS_VALUES:
            continue
        if cleaned.isupper() and len(cleaned) <= 16:
            continue
        if cleaned.lower() in blocked_values:
            continue
        if normalized_question and cleaned.lower() == normalized_question:
            continue
        if normalized_question and cleaned.lower().rstrip("?") == normalized_question.rstrip("?"):
            continue
        if normalized_question and normalized_question in cleaned.lower() and len(cleaned) <= len(normalized_question) + 3:
            continue
        if cleaned:
            return cleaned
    return None


def extract_summary(
    message: dict,
    query_result: Optional[Dict[str, Any]] = None,
    question: Optional[str] = None,
    blocked_values: Optional[set[str]] = None,
) -> tuple[str, Optional[str]]:
    candidates = []
    for attachment in message.get("attachments", []):
        text_entry = attachment.get("text")
        if isinstance(text_entry, dict):
            content = text_entry.get("content") or text_entry.get("text")
            if isinstance(content, str) and content.strip():
                return content.strip(), "text"
        elif isinstance(text_entry, str) and text_entry.strip():
            return text_entry.strip(), "text"

        markdown_entry = attachment.get("markdown")
        if isinstance(markdown_entry, dict):
            content = markdown_entry.get("content") or markdown_entry.get("text")
            if isinstance(content, str) and content.strip():
                return content.strip(), "markdown"
        elif isinstance(markdown_entry, str) and markdown_entry.strip():
            return markdown_entry.strip(), "markdown"

    for attachment in message.get("attachments", []):
        for key in ("text", "markdown", "content"):
            entry = attachment.get(key)
            if isinstance(entry, dict):
                content = entry.get("content") or entry.get("text")
                if isinstance(content, str):
                    candidates.append((key, content))
            elif isinstance(entry, str):
                candidates.append((key, entry))
        query_entry = attachment.get("query")
        if isinstance(query_entry, dict):
            description = query_entry.get("description")
            if isinstance(description, str):
                candidates.append(("description", description))

    candidates.extend(collect_texts(message))
    if query_result:
        candidates.extend(collect_texts(query_result))

    best = pick_best_text(candidates, question=question, blocked_values=blocked_values)
    if not best:
        return "Genie returned SQL, but no summary text was found.", None

    for key, value in candidates:
        if value.strip() == best:
            return best, key
    return best, None


def format_currency(value: float) -> str:
    return f"${value:,.0f}"


def format_percent(value: float) -> str:
    return f"{value * 100:.2f}%"


def parse_statement_row(query_result: Dict[str, Any]) -> tuple[list[str], list[Optional[str]]]:
    response = query_result.get("statement_response", {})
    manifest = response.get("manifest", {})
    schema = manifest.get("schema", {})
    columns = [col.get("name") for col in schema.get("columns", []) if col.get("name")]
    result = response.get("result", {})
    data = result.get("data_typed_array", [])
    if not data:
        return columns, []
    values = data[0].get("values", [])
    row = [value.get("str") if isinstance(value, dict) else None for value in values]
    return columns, row


def extract_table(query_result: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not query_result:
        return None
    response = query_result.get("statement_response", {})
    manifest = response.get("manifest", {})
    schema = manifest.get("schema", {})
    columns = [col.get("name") for col in schema.get("columns", []) if col.get("name")]
    if not columns:
        return None
    result = response.get("result", {})
    data = result.get("data_typed_array", [])
    if not data:
        return None
    rows = []
    for entry in data:
        values = entry.get("values", [])
        row = [value.get("str") if isinstance(value, dict) else None for value in values]
        rows.append(row)
    return {"columns": columns, "rows": rows}


def run_genie_sql(base_url: str, headers: Dict[str, str], sql: str) -> Optional[Dict[str, Any]]:
    now = time.time()
    with _GENIE_CACHE_LOCK:
        cached = _GENIE_CACHE.get(sql)
        if cached and now - cached["ts"] < GENIE_CACHE_TTL_SECONDS:
            return cached["table"]

    _GENIE_SEMAPHORE.acquire()
    try:
        for attempt in range(8):
            status_code, start_payload = api_request(
                f"{base_url}/start-conversation", "POST", {"content": sql}, headers
            )
            if status_code == 429:
                time.sleep(2**attempt)
                continue
            if status_code != 200:
                break
            break
        else:
            pass

        conversation_id = start_payload.get("conversation_id")
        message_id = start_payload.get("message_id")
        if not conversation_id or not message_id:
            raise RuntimeError("Genie conversation start failed.")

        message_payload = None
        for _ in range(40):
            status_code, message_payload = api_request(
                f"{base_url}/conversations/{conversation_id}/messages/{message_id}",
                "GET",
                None,
                headers,
            )
            if status_code == 429:
                time.sleep(1)
                continue
            if status_code != 200:
                raise RuntimeError("Genie message polling failed.")
            status = message_payload.get("status")
            if status in {"COMPLETED", "FAILED"}:
                break
            time.sleep(2)

        if not message_payload or message_payload.get("status") != "COMPLETED":
            raise RuntimeError("Genie message not completed.")

        status_code, query_result = api_request(
            f"{base_url}/conversations/{conversation_id}/messages/{message_id}/query-result",
            "GET",
            None,
            headers,
        )
        if status_code == 429:
            time.sleep(1)
            raise RuntimeError("Genie query result throttled.")

        table = extract_table(query_result)
        with _GENIE_CACHE_LOCK:
            _GENIE_CACHE[sql] = {"ts": time.time(), "table": table}
        return table
    except Exception:
        with _GENIE_CACHE_LOCK:
            cached = _GENIE_CACHE.get(sql)
            if cached:
                return cached["table"]
        return None
    finally:
        _GENIE_SEMAPHORE.release()


def run_direct_sql(sql: str) -> Optional[Dict[str, Any]]:
    """
    Execute SQL query against Databricks SQL Warehouse.
    Uses connection pool if available for better performance.
    """
    if dbsql is None:
        return None
    host = os.getenv("DATABRICKS_HOST")
    http_path = os.getenv("DATABRICKS_SQL_HTTP_PATH")
    token = os.getenv("DATABRICKS_TOKEN_FOR_SQL") or os.getenv("DATABRICKS_TOKEN_FOR_GENIE")
    if not host or not http_path or not token:
        return None

    cache_key = f"sql::{sql}"
    now = time.time()
    with _SQL_CACHE_LOCK:
        cached = _SQL_CACHE.get(cache_key)
        if cached and now - cached["ts"] < SQL_CACHE_TTL_SECONDS:
            return cached["table"]

    # Try connection pool first if available
    if _USE_POOL:
        try:
            rows = run_sql_with_pool(sql)
            if rows is not None:
                if not rows:
                    table = {"columns": [], "rows": []}
                else:
                    # Extract columns from first row
                    if hasattr(rows[0], 'asDict'):
                        columns = list(rows[0].asDict().keys())
                        formatted_rows = [[row.asDict().get(col) for col in columns] for row in rows]
                    else:
                        columns = [f"col_{i}" for i in range(len(rows[0]))]
                        formatted_rows = [list(row) for row in rows]
                    
                    table = {
                        "columns": columns,
                        "rows": [[None if value is None else str(value) for value in row] for row in formatted_rows],
                    }
                
                with _SQL_CACHE_LOCK:
                    _SQL_CACHE[cache_key] = {"ts": time.time(), "table": table}
                return table
        except Exception as e:
            logger.warning(f"Pool query failed, falling back to direct connection: {e}")

    # Fallback to direct connection
    try:
        with dbsql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall() or []
                columns = [col[0] for col in cursor.description] if cursor.description else []
        table = {
            "columns": columns,
            "rows": [[None if value is None else str(value) for value in row] for row in rows],
        }
        with _SQL_CACHE_LOCK:
            _SQL_CACHE[cache_key] = {"ts": time.time(), "table": table}
        return table
    except Exception:
        return None


def get_cached_dashboard_payload(cache_key: str) -> Optional[Dict[str, Any]]:
    now = time.time()
    with _DASHBOARD_CACHE_LOCK:
        cached = _DASHBOARD_CACHE.get(cache_key)
        if cached and now - cached["ts"] < DASHBOARD_CACHE_TTL_SECONDS:
            return cached["payload"]
    return None


def set_cached_dashboard_payload(cache_key: str, payload: Dict[str, Any]) -> None:
    with _DASHBOARD_CACHE_LOCK:
        _DASHBOARD_CACHE[cache_key] = {"ts": time.time(), "payload": payload}


def first_missing_table(tables: Dict[str, Optional[Dict[str, Any]]]) -> Optional[str]:
    for name, table in tables.items():
        if table is None:
            return name
    return None


def build_summary_from_result(question: str, query_result: Optional[Dict[str, Any]]) -> Optional[str]:
    if not query_result:
        return None
    columns, row = parse_statement_row(query_result)
    if not columns or not row:
        return None

    column_map = {name: row[idx] if idx < len(row) else None for idx, name in enumerate(columns)}
    revenue_growth = column_map.get("revenue_growth")
    total_revenue = column_map.get("total_revenue")
    total_prior_revenue = column_map.get("total_prior_revenue")
    quarter = column_map.get("quarter")

    if revenue_growth is not None:
        try:
            growth_value = float(revenue_growth)
        except (TypeError, ValueError):
            return None

        summary = f"Revenue growth last quarter was {format_percent(growth_value)}."
        detail_bits = []
        try:
            if total_revenue is not None:
                detail_bits.append(f"total revenue {format_currency(float(total_revenue))}")
            if total_prior_revenue is not None:
                detail_bits.append(f"prior revenue {format_currency(float(total_prior_revenue))}")
        except (TypeError, ValueError):
            detail_bits = []

        if quarter:
            detail_bits.insert(0, f"quarter starting {quarter[:10]}")

        if detail_bits:
            summary = f"{summary} ({', '.join(detail_bits)}.)"
        return summary

    if total_revenue is not None:
        try:
            revenue_value = float(total_revenue)
        except (TypeError, ValueError):
            return None

        summary = (
            f"The total revenue for the last quarter was {format_currency(revenue_value)}. "
            "This figure represents all recorded revenue for that quarter."
        )
        if quarter:
            summary = f"{summary} (quarter starting {quarter[:10]}.)"
        return summary

    return None


def is_poor_summary(summary: str) -> bool:
    cleaned = summary.strip()
    if not cleaned:
        return True
    if cleaned.lower().startswith("genie returned"):
        return True
    if cleaned.endswith("?"):
        return True
    if is_probably_metric_name(cleaned):
        return True
    return False


def find_sql(payload: Any) -> Optional[str]:
    if isinstance(payload, dict):
        for value in payload.values():
            sql_text = find_sql(value)
            if sql_text:
                return sql_text
    elif isinstance(payload, list):
        for item in payload:
            sql_text = find_sql(item)
            if sql_text:
                return sql_text
    elif isinstance(payload, str) and "select" in payload.lower():
        return payload
    return None


def extract_sql(message: Dict[str, Any], query_result: Optional[Dict[str, Any]]) -> Optional[str]:
    candidate = message.get("query") or message.get("sql")
    if isinstance(candidate, str):
        return candidate
    return find_sql(message) or find_sql(query_result or {})


class AppHandler(BaseHTTPRequestHandler):
    def _send_json(self, status: int, payload: dict) -> None:
        """Send JSON response with optional gzip compression."""
        body = json.dumps(payload).encode("utf-8")
        
        # Enable compression for responses > 1KB if client supports it
        accept_encoding = self.headers.get("Accept-Encoding", "")
        should_compress = "gzip" in accept_encoding and len(body) > 1024
        
        if should_compress:
            compressed_body = gzip.compress(body, compresslevel=6)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Length", str(len(compressed_body)))
            self.send_header("Vary", "Accept-Encoding")
            self.end_headers()
            self.wfile.write(compressed_body)
            logger.debug(f"Compressed response: {len(body)} -> {len(compressed_body)} bytes ({100 * len(compressed_body) / len(body):.1f}%)")
        else:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def _send_file(self, file_path: Path) -> None:
        if not file_path.exists():
            self.send_response(404)
            self.end_headers()
            return
        content_type, _ = mimetypes.guess_type(str(file_path))
        content_type = content_type or "application/octet-stream"
        data = file_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self) -> None:
        if self.path != "/api/genie/query":
            self._send_json(404, {"error": "Not found"})
            return

        try:
            content_length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(content_length) or "{}")
            question = payload.get("question", "").strip()
            if not question:
                self._send_json(400, {"error": "Question cannot be empty."})
                return

            host = os.getenv("DATABRICKS_HOST")
            token = os.getenv("DATABRICKS_TOKEN_FOR_GENIE")
            space_id = os.getenv("GENIE_SPACE_ID")
            if not host or not token or not space_id:
                self._send_json(500, {"error": "Missing Genie configuration env vars."})
                return

            base_url = f"https://{host}/api/2.0/genie/spaces/{space_id}"
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

            status_code, start_payload = api_request(
                f"{base_url}/start-conversation", "POST", {"content": question}, headers
            )
            if status_code != 200:
                self._send_json(status_code, {"error": "Failed to start Genie conversation."})
                return

            conversation_id = start_payload.get("conversation_id")
            message_id = start_payload.get("message_id")
            if not conversation_id or not message_id:
                self._send_json(500, {"error": "Invalid response from Genie."})
                return

            message_payload = None
            for _ in range(40):
                status_code, message_payload = api_request(
                    f"{base_url}/conversations/{conversation_id}/messages/{message_id}",
                    "GET",
                    None,
                    headers,
                )
                if status_code != 200:
                    self._send_json(status_code, {"error": "Failed to poll Genie status."})
                    return
                status = message_payload.get("status")
                if status in {"COMPLETED", "FAILED"}:
                    break
                time.sleep(2)

            if not message_payload or message_payload.get("status") != "COMPLETED":
                self._send_json(500, {"error": "Genie query failed."})
                return

            _, query_result = api_request(
                f"{base_url}/conversations/{conversation_id}/messages/{message_id}/query-result",
                "GET",
                None,
                headers,
            )

            blocked_values = {conversation_id, message_id}
            summary, summary_source = extract_summary(
                message_payload,
                query_result,
                question=question,
                blocked_values=blocked_values,
            )
            if is_poor_summary(summary) or summary_source == "description":
                fallback = build_summary_from_result(question, query_result)
                if fallback:
                    summary = fallback
            table = extract_table(query_result)

            self._send_json(200, {"summary": summary, "table": table})
        except Exception:  # pragma: no cover
            logger.exception("Unhandled error processing Genie query.")
            self._send_json(500, {"error": "An unexpected error occurred. Please try again."})

    def do_GET(self) -> None:
        if self.path.startswith("/api/"):
            if self.path == "/api/user":
                self._handle_user()
                return
            if self.path == "/api/dashboard/kpis":
                self._handle_kpis()
                return
            if self.path == "/api/dashboard/charts":
                self._handle_charts()
                return
            if self.path == "/api/dashboard/revenue":
                self._handle_revenue()
                return
            if self.path == "/api/dashboard/operations":
                self._handle_operations()
                return
            if self.path == "/api/dashboard/customers":
                self._handle_customers()
                return
            if self.path == "/api/dashboard/map":
                self._handle_map()
                return
            self._send_json(404, {"error": "Not found"})
            return

        if not DIST_DIR.exists():
            self._send_json(500, {"error": "dist/ folder not found."})
            return

        if self.path == "/" or self.path == "":
            self._send_file(DIST_DIR / "index.html")
            return

        requested = (DIST_DIR / self.path.lstrip("/")).resolve()
        if DIST_DIR in requested.parents and requested.is_file():
            self._send_file(requested)
            return

        self._send_file(DIST_DIR / "index.html")
    def _get_genie_context(self) -> tuple[str, Dict[str, str]]:
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN_FOR_GENIE")
        space_id = os.getenv("GENIE_SPACE_ID")
        if not host or not token or not space_id:
            raise RuntimeError("Missing Genie configuration env vars.")
        base_url = f"https://{host}/api/2.0/genie/spaces/{space_id}"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        return base_url, headers

    def _handle_kpis(self) -> None:
        try:
            cache_key = "dashboard:kpis"
            cached = get_cached_dashboard_payload(cache_key)
            if cached is not None:
                self._send_json(200, cached)
                return
            kpis_sql = (
                "WITH sales AS ("
                "SELECT *, MAX(date) OVER() AS max_date "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched"
                ") "
                "SELECT "
                "(SELECT SUM(CASE "
                "WHEN date >= date_trunc('month', max_date) "
                "AND date < add_months(date_trunc('month', max_date), 1) "
                "THEN total_amount END) FROM sales) AS total_revenue, "
                "(SELECT AVG(satisfaction_score) FROM sales) AS avg_satisfaction, "
                "(SELECT SUM(CASE "
                "WHEN category = 'Tire' "
                "AND date >= date_trunc('month', max_date) "
                "AND date < add_months(date_trunc('month', max_date), 1) "
                "THEN quantity END) FROM sales) AS tire_units, "
                "(SELECT COUNT(*) "
                "FROM kaustavpaul_demo.dtc_demo.inventory "
                "WHERE stock_qty <= reorder_threshold) AS low_stock_items, "
                "(SELECT revenue_growth "
                "FROM (SELECT *, MAX(month) OVER() AS max_month "
                "FROM kaustavpaul_demo.dtc_demo.vw_revenue_growth) t "
                "WHERE month = max_month) AS revenue_growth, "
                "(SELECT MAX(max_date) FROM sales) AS max_date"
            )
            kpis = run_direct_sql(kpis_sql)
            if kpis is None:
                self._send_json(503, {"error": "Dashboard data unavailable. Please try again."})
                return
            payload = {
                "totalRevenue": parse_float(table_first_value(kpis, "total_revenue")),
                "revenueGrowth": parse_float(table_first_value(kpis, "revenue_growth")),
                "avgSatisfaction": parse_float(table_first_value(kpis, "avg_satisfaction")),
                "tireUnits": parse_float(table_first_value(kpis, "tire_units")),
                "inventoryRisk": parse_float(table_first_value(kpis, "low_stock_items")),
                "currentMonthLabel": format_month_label(table_first_value(kpis, "max_date")),
            }
            set_cached_dashboard_payload(cache_key, payload)
            self._send_json(200, payload)
        except Exception:  # pragma: no cover
            logger.exception("Unhandled error in KPI handler.")
            self._send_json(500, {"error": "An unexpected error occurred. Please try again."})

    def _handle_charts(self) -> None:
        try:
            cache_key = "dashboard:charts"
            cached = get_cached_dashboard_payload(cache_key)
            if cached is not None:
                self._send_json(200, cached)
                return
            revenue_trend_sql = (
                "SELECT date_trunc('month', date) AS month, SUM(total_amount) AS revenue "
                "FROM (SELECT *, MAX(date) OVER() AS max_date "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched) s "
                "WHERE date >= add_months(date_trunc('month', max_date), -5) "
                "GROUP BY date_trunc('month', date) "
                "ORDER BY month"
            )
            top_tires_sql = (
                "SELECT product_name AS model, SUM(quantity) AS units "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched "
                "WHERE category = 'Tire' "
                "GROUP BY product_name "
                "ORDER BY units DESC "
                "LIMIT 5"
            )
            inventory_health_sql = (
                "SELECT store_name AS store, "
                "SUM(CASE WHEN quantity > 1 THEN quantity ELSE 0 END) AS healthy, "
                "SUM(CASE WHEN quantity = 1 THEN 1 ELSE 0 END) AS low, "
                "SUM(CASE WHEN quantity = 0 THEN 1 ELSE 0 END) AS critical "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched "
                "GROUP BY store_name "
                "ORDER BY store_name"
            )
            satisfaction_sql = (
                "SELECT customer_region AS region, AVG(satisfaction_score) AS score "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched "
                "GROUP BY customer_region "
                "ORDER BY score DESC"
            )
            revenue_trend = run_direct_sql(revenue_trend_sql)
            top_tires = run_direct_sql(top_tires_sql)
            inventory_health = run_direct_sql(inventory_health_sql)
            satisfaction = run_direct_sql(satisfaction_sql)
            missing = first_missing_table(
                {
                    "revenue_trend": revenue_trend,
                    "top_tires": top_tires,
                    "inventory_health": inventory_health,
                    "satisfaction": satisfaction,
                }
            )
            if missing:
                self._send_json(503, {"error": "Dashboard data unavailable. Please try again."})
                return
            payload = {
                "revenueTrend": table_to_dicts(revenue_trend),
                "topTires": table_to_dicts(top_tires),
                "inventoryHealth": table_to_dicts(inventory_health),
                "satisfactionByRegion": table_to_dicts(satisfaction),
            }
            set_cached_dashboard_payload(cache_key, payload)
            self._send_json(200, payload)
        except Exception:  # pragma: no cover
            logger.exception("Unhandled error in charts handler.")
            self._send_json(500, {"error": "An unexpected error occurred. Please try again."})

    def _handle_revenue(self) -> None:
        try:
            cache_key = "dashboard:revenue"
            cached = get_cached_dashboard_payload(cache_key)
            if cached is not None:
                self._send_json(200, cached)
                return
            monthly_sql = (
                "SELECT month, revenue, "
                "revenue * 1.03 AS target, revenue * 0.92 AS last_year "
                "FROM (SELECT *, MAX(month) OVER() AS max_month "
                "FROM kaustavpaul_demo.dtc_demo.vw_revenue_growth) t "
                "WHERE month >= add_months(date_trunc('month', max_month), -5) "
                "ORDER BY month"
            )
            regional_sql = (
                "SELECT store_region AS region, quarter(date) AS quarter, "
                "SUM(total_amount) AS revenue "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched "
                "GROUP BY store_region, quarter(date) "
                "ORDER BY store_region, quarter(date)"
            )
            category_sql = (
                "SELECT category, SUM(total_amount) AS amount "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched "
                "GROUP BY category "
                "ORDER BY amount DESC"
            )
            stats_sql = (
                "SELECT "
                "SUM(CASE WHEN date >= date_trunc('month', max_date) "
                "AND date < add_months(date_trunc('month', max_date), 1) THEN total_amount ELSE 0 END) "
                "AS current_month_revenue, "
                "SUM(CASE WHEN date >= date_trunc('year', max_date) THEN total_amount ELSE 0 END) "
                "AS ytd_revenue "
                "FROM (SELECT *, MAX(date) OVER() AS max_date "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched) s"
            )
            quarterly_growth_sql = (
                "SELECT AVG(revenue_growth) AS quarterly_growth "
                "FROM (SELECT *, MAX(month) OVER() AS max_month "
                "FROM kaustavpaul_demo.dtc_demo.vw_revenue_growth) t "
                "WHERE month >= date_trunc('quarter', max_month)"
            )
            top_region_sql = (
                "SELECT store_region AS region, SUM(total_amount) AS revenue "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched "
                "GROUP BY store_region "
                "ORDER BY revenue DESC "
                "LIMIT 1"
            )
            current_month_sql = "SELECT MAX(date) AS max_date FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched"
            monthly = run_direct_sql(monthly_sql)
            regional = run_direct_sql(regional_sql)
            category = run_direct_sql(category_sql)
            stats = run_direct_sql(stats_sql)
            quarterly_growth = run_direct_sql(quarterly_growth_sql)
            top_region = run_direct_sql(top_region_sql)
            current_month = run_direct_sql(current_month_sql)
            missing = first_missing_table(
                {
                    "monthly": monthly,
                    "regional": regional,
                    "category": category,
                    "stats": stats,
                    "quarterly_growth": quarterly_growth,
                    "top_region": top_region,
                    "current_month": current_month,
                }
            )
            if missing:
                self._send_json(503, {"error": "Dashboard data unavailable. Please try again."})
                return
            stats_row = table_to_dicts(stats)
            top_region_row = table_to_dicts(top_region)
            payload = {
                "monthly": table_to_dicts(monthly),
                "regional": table_to_dicts(regional),
                "category": table_to_dicts(category),
                "currentMonthLabel": format_month_label(table_first_value(current_month, "max_date")),
                "stats": {
                    "currentMonthRevenue": parse_float(stats_row[0].get("current_month_revenue")) if stats_row else None,
                    "ytdRevenue": parse_float(stats_row[0].get("ytd_revenue")) if stats_row else None,
                    "quarterlyGrowth": parse_float(table_first_value(quarterly_growth, "quarterly_growth")),
                    "topRegion": top_region_row[0].get("region") if top_region_row else None,
                },
            }
            set_cached_dashboard_payload(cache_key, payload)
            self._send_json(200, payload)
        except Exception:  # pragma: no cover
            logger.exception("Unhandled error in revenue handler.")
            self._send_json(500, {"error": "An unexpected error occurred. Please try again."})

    def _handle_operations(self) -> None:
        try:
            cache_key = "dashboard:operations"
            cached = get_cached_dashboard_payload(cache_key)
            if cached is not None:
                self._send_json(200, cached)
                return
            inventory_by_store_sql = (
                "SELECT store_name AS store, "
                "SUM(quantity) AS available, "
                "0 AS reserved, "
                "SUM(CASE WHEN quantity = 1 THEN 1 ELSE 0 END) AS low_stock "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched "
                "GROUP BY store_name "
                "ORDER BY store_name"
            )
            turnover_sql = (
                "SELECT date_trunc('month', date) AS month, "
                "SUM(quantity) AS turnover "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched "
                "GROUP BY date_trunc('month', date) "
                "ORDER BY month"
            )
            critical_items_sql = (
                "SELECT product_name AS item, SUM(quantity) AS current_stock, "
                "10 AS reorder_point, "
                "CASE WHEN SUM(quantity) <= 5 THEN 'Critical' ELSE 'Low' END AS status "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched "
                "GROUP BY product_name "
                "ORDER BY SUM(quantity) ASC "
                "LIMIT 10"
            )
            store_performance_sql = (
                "SELECT store_name AS store, "
                "ROUND(100 * revenue / max_revenue, 0) AS efficiency, "
                "ROUND(avg_satisfaction, 1) AS satisfaction, "
                "units AS throughput "
                "FROM ("
                "SELECT store_name, "
                "SUM(total_amount) AS revenue, "
                "SUM(quantity) AS units, "
                "AVG(satisfaction_score) AS avg_satisfaction, "
                "MAX(SUM(total_amount)) OVER() AS max_revenue "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched "
                "GROUP BY store_name"
                ") t"
            )
            metrics_sql = (
                "SELECT "
                "SUM(quantity) AS total_units, "
                "SUM(CASE WHEN quantity = 1 THEN 1 ELSE 0 END) AS critical_items, "
                "COUNT(DISTINCT store_id) AS active_stores "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched"
            )
            inventory_by_store = run_direct_sql(inventory_by_store_sql)
            turnover = run_direct_sql(turnover_sql)
            critical_items = run_direct_sql(critical_items_sql)
            store_performance = run_direct_sql(store_performance_sql)
            metrics = run_direct_sql(metrics_sql)
            missing = first_missing_table(
                {
                    "inventory_by_store": inventory_by_store,
                    "turnover": turnover,
                    "critical_items": critical_items,
                    "store_performance": store_performance,
                    "metrics": metrics,
                }
            )
            if missing:
                self._send_json(503, {"error": "Dashboard data unavailable. Please try again."})
                return
            metrics_row = table_to_dicts(metrics)
            payload = {
                "inventoryByStore": table_to_dicts(inventory_by_store),
                "stockTurnover": table_to_dicts(turnover),
                "criticalItems": table_to_dicts(critical_items),
                "storePerformance": table_to_dicts(store_performance),
                "metrics": metrics_row[0] if metrics_row else {},
            }
            set_cached_dashboard_payload(cache_key, payload)
            self._send_json(200, payload)
        except Exception:  # pragma: no cover
            logger.exception("Unhandled error in operations handler.")
            self._send_json(500, {"error": "An unexpected error occurred. Please try again."})

    def _handle_customers(self) -> None:
        try:
            cache_key = "dashboard:customers"
            cached = get_cached_dashboard_payload(cache_key)
            if cached is not None:
                self._send_json(200, cached)
                return
            satisfaction_trend_sql = (
                "SELECT date_trunc('month', date) AS month, "
                "AVG(satisfaction_score) AS score, "
                "COUNT(*) AS responses "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched "
                "GROUP BY date_trunc('month', date) "
                "ORDER BY month"
            )
            regional_satisfaction_sql = (
                "SELECT customer_region AS region, AVG(satisfaction_score) AS score, COUNT(*) AS surveys "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched "
                "GROUP BY customer_region "
                "ORDER BY score DESC"
            )
            service_breakdown_sql = (
                "SELECT product_name AS name, COUNT(*) AS value "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched "
                "WHERE category = 'Service' "
                "GROUP BY product_name "
                "ORDER BY value DESC"
            )
            nps_breakdown_sql = (
                "SELECT CASE "
                "WHEN satisfaction_score >= 4.5 THEN 'Promoter' "
                "WHEN satisfaction_score >= 4.0 THEN 'Passive' "
                "ELSE 'Detractor' END AS category, "
                "COUNT(*) AS count "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched "
                "GROUP BY CASE "
                "WHEN satisfaction_score >= 4.5 THEN 'Promoter' "
                "WHEN satisfaction_score >= 4.0 THEN 'Passive' "
                "ELSE 'Detractor' END"
            )
            feedback_topics_sql = (
                "SELECT category AS topic, "
                "CASE WHEN AVG(satisfaction_score) >= 4.4 THEN 'positive' ELSE 'neutral' END AS sentiment, "
                "COUNT(*) AS mentions "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched "
                "GROUP BY category"
            )
            metrics_sql = (
                "SELECT "
                "AVG(satisfaction_score) AS overall_satisfaction, "
                "COUNT(*) AS total_surveys "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched"
            )
            repeat_rate_sql = (
                "SELECT "
                "COUNT(DISTINCT CASE WHEN sales_per_customer > 1 THEN customer_id END) * 1.0 "
                "/ COUNT(DISTINCT customer_id) AS repeat_rate "
                "FROM (SELECT customer_id, COUNT(*) AS sales_per_customer "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched "
                "GROUP BY customer_id) t"
            )
            active_feedback_sql = (
                "SELECT COUNT(*) AS active_feedback "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched "
                "WHERE category = 'Service'"
            )
            satisfaction_trend = run_direct_sql(satisfaction_trend_sql)
            regional_satisfaction = run_direct_sql(regional_satisfaction_sql)
            service_breakdown = run_direct_sql(service_breakdown_sql)
            nps_breakdown = run_direct_sql(nps_breakdown_sql)
            feedback_topics = run_direct_sql(feedback_topics_sql)
            metrics = run_direct_sql(metrics_sql)
            repeat_rate = run_direct_sql(repeat_rate_sql)
            active_feedback = run_direct_sql(active_feedback_sql)
            missing = first_missing_table(
                {
                    "satisfaction_trend": satisfaction_trend,
                    "regional_satisfaction": regional_satisfaction,
                    "service_breakdown": service_breakdown,
                    "nps_breakdown": nps_breakdown,
                    "feedback_topics": feedback_topics,
                    "metrics": metrics,
                    "repeat_rate": repeat_rate,
                    "active_feedback": active_feedback,
                }
            )
            if missing:
                self._send_json(503, {"error": "Dashboard data unavailable. Please try again."})
                return
            metrics_row = table_to_dicts(metrics)
            payload = {
                "satisfactionTrend": table_to_dicts(satisfaction_trend),
                "regionalSatisfaction": table_to_dicts(regional_satisfaction),
                "serviceBreakdown": table_to_dicts(service_breakdown),
                "npsBreakdown": table_to_dicts(nps_breakdown),
                "feedbackTopics": table_to_dicts(feedback_topics),
                "metrics": {
                    "overallSatisfaction": parse_float(metrics_row[0].get("overall_satisfaction")) if metrics_row else None,
                    "totalSurveys": parse_float(metrics_row[0].get("total_surveys")) if metrics_row else None,
                    "repeatRate": parse_float(table_first_value(repeat_rate, "repeat_rate")),
                    "activeFeedback": parse_float(table_first_value(active_feedback, "active_feedback")),
                },
            }
            set_cached_dashboard_payload(cache_key, payload)
            self._send_json(200, payload)
        except Exception:  # pragma: no cover
            logger.exception("Unhandled error in customers handler.")
            self._send_json(500, {"error": "An unexpected error occurred. Please try again."})

    def _handle_user(self) -> None:
        """Return authenticated user information from Databricks App context."""
        try:
            # Databricks Apps inject user context via X-Forwarded headers
            user_email = self.headers.get("X-Forwarded-Email", "")
            user_name = self.headers.get("X-Forwarded-Preferred-Username", "")
            
            # Fallback to environment or default if headers not present
            if not user_email:
                user_email = os.getenv("USER_EMAIL", "executive@discounttire.com")
            if not user_name:
                user_name = os.getenv("USER_NAME", "Executive User")
            
            # Extract first/last name if email format is first.last@domain
            display_name = user_name
            if not user_name or user_name == user_email:
                # Try to derive name from email
                local_part = user_email.split("@")[0] if "@" in user_email else user_email
                name_parts = local_part.replace(".", " ").replace("_", " ").title().split()
                display_name = " ".join(name_parts) if name_parts else "Executive User"
            
            self._send_json(200, {
                "name": display_name,
                "email": user_email,
                "role": "Executive Viewer"
            })
        except Exception:  # pragma: no cover
            logger.exception("Unhandled error in user handler.")
            self._send_json(500, {"error": "An unexpected error occurred. Please try again."})

    def _handle_map(self) -> None:
        try:
            cache_key = "dashboard:map"
            cached = get_cached_dashboard_payload(cache_key)
            if cached is not None:
                self._send_json(200, cached)
                return
            store_locations_sql = (
                "WITH sales_rollup AS ("
                "SELECT store_id, SUM(total_amount) AS revenue, SUM(quantity) AS units "
                "FROM kaustavpaul_demo.dtc_demo.vw_sales_enriched "
                "GROUP BY store_id"
                ") "
                "SELECT st.store_id, st.store_name, st.region AS store_region, st.state, "
                "COALESCE(sr.revenue, 0) AS revenue, COALESCE(sr.units, 0) AS units, "
                "CASE st.state "
                "WHEN 'AZ' THEN 33.4484 WHEN 'TX' THEN 30.2672 WHEN 'CA' THEN 34.0522 "
                "WHEN 'CO' THEN 39.7392 WHEN 'FL' THEN 27.9944 WHEN 'GA' THEN 33.7490 "
                "WHEN 'NC' THEN 35.7796 WHEN 'TN' THEN 36.1627 WHEN 'IL' THEN 41.8781 "
                "WHEN 'OH' THEN 39.9612 ELSE 39.8283 END AS latitude, "
                "CASE st.state "
                "WHEN 'AZ' THEN -112.0740 WHEN 'TX' THEN -97.7431 WHEN 'CA' THEN -118.2437 "
                "WHEN 'CO' THEN -104.9903 WHEN 'FL' THEN -81.7603 WHEN 'GA' THEN -84.3880 "
                "WHEN 'NC' THEN -78.6382 WHEN 'TN' THEN -86.7816 WHEN 'IL' THEN -87.6298 "
                "WHEN 'OH' THEN -82.9988 ELSE -98.5795 END AS longitude "
                "FROM kaustavpaul_demo.dtc_demo.stores st "
                "LEFT JOIN sales_rollup sr ON st.store_id = sr.store_id"
            )
            locations = run_direct_sql(store_locations_sql)
            if locations is None:
                self._send_json(503, {"error": "Dashboard data unavailable. Please try again."})
                return
            payload = {"locations": table_to_dicts(locations)}
            set_cached_dashboard_payload(cache_key, payload)
            self._send_json(200, payload)
        except Exception:  # pragma: no cover
            logger.exception("Unhandled error in map handler.")
            self._send_json(500, {"error": "An unexpected error occurred. Please try again."})


def main() -> None:
    port = int(os.getenv("DATABRICKS_APP_PORT", "8000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), AppHandler)
    print(f"Serving on port {port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
