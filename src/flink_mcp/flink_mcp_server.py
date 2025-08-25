from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional, List

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from .flink_sql_gateway_client import FlinkSqlGatewayClient


def build_server() -> FastMCP:
    load_dotenv()

    server = FastMCP("Flink SQLGateway MCP Server v0.2.0")

    client = FlinkSqlGatewayClient(os.getenv("SQL_GATEWAY_API_BASE_URL"))

    # Server-managed session and stream registry
    # _stream_index maps jobId -> { session, operation, nextToken }
    _session_handle: Optional[str] = None
    _stream_index: Dict[str, Dict[str, Any]] = {}

    def _ensure_session() -> str:
        nonlocal _session_handle
        if _session_handle:
            return _session_handle
        resp = client.open_session({})
        handle = resp.get("sessionHandle") or (resp.get("session") or {}).get("handle")
        if not isinstance(handle, str):
            raise RuntimeError("Failed to open Flink SQL Gateway session")
        _session_handle = handle
        return _session_handle

    def _poll_status(session_handle: str, operation_handle: str, timeout: float, interval: float) -> str:
        end = time.time() + timeout
        while time.time() < end:
            st = client.get_operation_status(session_handle, operation_handle)
            status = str(st.get("status") or st.get("operationStatus") or "").upper()
            if status in {"FINISHED", "ERROR", "CANCELED", "CLOSED"}:
                return status
            time.sleep(interval)
        return "TIMEOUT"

    def _extract_job_id(page: Dict[str, Any]) -> Optional[str]:
        j = page.get("jobID") or page.get("jobId")
        return j if isinstance(j, str) else None

    @server.resource("https://mcp.local/flink/info")
    def flink_info() -> Dict[str, Any]:
        """Return basic cluster information from the SQL Gateway /v1/info endpoint."""
        return client.get_info()

    @server.tool()
    def get_config() -> Dict[str, Any]:
        """Return current session configuration (properties) for the managed session."""
        session_handle = _ensure_session()
        return client.get_session(session_handle)
    # Expose for tests
    setattr(server, "get_config", get_config)

    @server.tool()
    def configure_session(statement: str) -> Dict[str, Any]:
        """Apply a single session-scoped DDL/config statement (CREATE/USE/SET/RESET/etc.)."""
        session_handle = _ensure_session()
        return client.configure_session(session_handle=session_handle, statement=statement)
    setattr(server, "configure_session", configure_session)

    @server.tool()
    def run_query_collect_and_stop(
        query: str,
        max_rows: int = 5,
        max_seconds: float = 15.0,
    ) -> Dict[str, Any]:
        """Run a short-lived query, fetch up to max_rows within max_seconds, then stop the job if present."""
        session_handle = _ensure_session()
        deadline = time.time() + max_seconds

        exec_resp = client.execute_statement(session_handle, query)
        op = exec_resp.get("operationHandle") or exec_resp.get("operation_handle")
        if isinstance(op, dict):
            op = op.get("identifier") or op.get("handle") or op.get("id")
        if not isinstance(op, str):
            return {"errorType": "NO_OPERATION_HANDLE", "message": "execute returned no handle"}

        status = _poll_status(session_handle, op, max(0.0, deadline - time.time()), 0.5)
        if status != "FINISHED":
            return {"errorType": f"OPERATION_{status}", "message": "operation did not finish successfully"}

        pages: List[Dict[str, Any]] = []
        rows_collected = 0
        token = 0
        jid: Optional[str] = None

        while rows_collected < max_rows and time.time() < deadline:
            page = client.fetch_result(session_handle, op, token=token)
            pages.append(page)
            if jid is None:
                jid = _extract_job_id(page)
            rtype = str(page.get("resultType") or "").upper()
            if rtype == "NOT_READY":
                time.sleep(0.25)
                continue
            data = (page.get("results") or {}).get("data") or []
            rows_collected += len(data) if isinstance(data, list) else 0
            token += 1
            if rtype == "EOS":
                break

        stop_result: Optional[Dict[str, Any]] = None
        if jid:
            stop_exec = client.execute_statement(session_handle, f"STOP JOB {jid}")
            stop_op = stop_exec.get("operationHandle") or stop_exec.get("operation_handle")
            if isinstance(stop_op, dict):
                stop_op = stop_op.get("identifier") or stop_op.get("handle") or stop_op.get("id")
            if isinstance(stop_op, str):
                _ = _poll_status(session_handle, stop_op, 30.0, 0.5)
            stop_result = {"ok": True}

        try:
            client.close_operation(session_handle, op)
        except Exception:
            pass

        out: Dict[str, Any] = {
            "jobID": jid,
            "pages": pages,
            "rowsCollected": rows_collected,
            "nextToken": token,
        }
        if stop_result is not None:
            out["stopResult"] = stop_result
        return out
    setattr(server, "run_query_collect_and_stop", run_query_collect_and_stop)

    @server.tool()
    def run_query_stream_start(query: str) -> Dict[str, Any]:
        """Start a streaming query and return its cluster jobID; leaves the job running."""
        session_handle = _ensure_session()

        exec_resp = client.execute_statement(session_handle, query)
        op = exec_resp.get("operationHandle") or exec_resp.get("operation_handle")
        if isinstance(op, dict):
            op = op.get("identifier") or op.get("handle") or op.get("id")
        if not isinstance(op, str):
            return {"errorType": "NO_OPERATION_HANDLE", "message": "execute returned no handle"}

        status = _poll_status(session_handle, op, 60.0, 0.5)
        if status != "FINISHED":
            return {"errorType": f"OPERATION_{status}", "message": "operation did not finish successfully"}

        page0 = client.fetch_result(session_handle, op, token=0)
        jid = _extract_job_id(page0)
        if not isinstance(jid, str):
            return {"errorType": "JOB_ID_NOT_AVAILABLE", "message": "job id not present in results"}

        _stream_index[jid] = {"session": session_handle, "operation": op, "nextToken": 1}
        return {"jobID": jid}
    setattr(server, "run_query_stream_start", run_query_stream_start)

    @server.tool()
    def cancel_job(job_id: str) -> Dict[str, Any]:
        """Issue STOP JOB <job_id> and remove internal tracking state for that job."""
        session_handle = _ensure_session()

        stop_exec = client.execute_statement(session_handle, f"STOP JOB {job_id}")
        stop_op = stop_exec.get("operationHandle") or stop_exec.get("operation_handle")
        if isinstance(stop_op, dict):
            stop_op = stop_op.get("identifier") or stop_op.get("handle") or stop_op.get("id")
        if isinstance(stop_op, str):
            _ = _poll_status(session_handle, stop_op, 30.0, 0.5)

        _stream_index.pop(job_id, None)
        return {"jobID": job_id, "status": "STOP_SUBMITTED"}
    setattr(server, "cancel_job", cancel_job)

    @server.tool()
    def fetch_result_by_jobid(job_id: str) -> Dict[str, Any]:
        """Fetch a single page for a tracked job using a single shared cursor per job."""
        stream = _stream_index.get(job_id)
        if not stream:
            return {"errorType": "UNKNOWN_JOB", "message": "job not tracked"}
        session_handle = stream["session"]
        operation_handle = stream["operation"]
        token = int(stream.get("nextToken", 0))

        page = client.fetch_result(session_handle, operation_handle, token=token)
        token += 1
        rtype = str(page.get("resultType") or "").upper()
        is_end = rtype == "EOS"

        stream["nextToken"] = max(int(stream.get("nextToken", 0)), token)
        return {"page": page, "nextToken": token, "isEnd": is_end}
    setattr(server, "fetch_result_by_jobid", fetch_result_by_jobid)

    return server


def main() -> None:
    server = build_server()
    server.run(transport="stdio")


if __name__ == "__main__":
    main()




