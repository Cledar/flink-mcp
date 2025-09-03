from __future__ import annotations

import logging
import os
import time
import asyncio
from typing import Any
import importlib.metadata

import httpx
from dotenv import load_dotenv
from fastmcp import FastMCP

from .flink_sql_gateway_client import FlinkSqlGatewayClient


def build_server(
    base_url: str | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> FastMCP:
    load_dotenv()

    version = importlib.metadata.version("flink-mcp")
    server = FastMCP(f"Flink SQLGateway MCP Server v{version}")

    logger = logging.getLogger(__name__)

    # Allow tests to inject a mocked HTTP client and/or base URL.
    effective_base_url = base_url or os.getenv("SQL_GATEWAY_API_BASE_URL")
    client = FlinkSqlGatewayClient(effective_base_url, client=http_client)

    async def _poll_status(
        session_handle: str, operation_handle: str, timeout: float, interval: float
    ) -> tuple[str, dict[str, Any]]:
        end = time.time() + timeout
        last_payload: dict[str, Any] = {}
        while time.time() < end:
            last_payload = await client.get_operation_status(
                session_handle, operation_handle
            )
            status = str(last_payload.get("status", "")).upper()
            if status in {"FINISHED", "ERROR", "CANCELED", "CLOSED"}:
                return status, last_payload
            await asyncio.sleep(interval)
        return "TIMEOUT", last_payload

    def _extract_job_id(page: dict[str, Any]) -> str | None:
        j = page.get("jobID") or page.get("jobId")
        return j if isinstance(j, str) else None

    async def _job_status(session_handle: str, job_id: str) -> str | None:
        """Return current cluster job status via DESCRIBE JOB, or None if unavailable.

        Extracts the value of the "status" column from the first row of the result.
        """
        try:
            exec_resp = await client.execute_statement(
                session_handle, f"DESCRIBE JOB '{job_id}'"
            )
            op = exec_resp.get("operationHandle") or exec_resp.get("operation_handle")
            if isinstance(op, dict):
                op = op.get("identifier") or op.get("handle") or op.get("id")
            if not isinstance(op, str):
                return None
            status, _ = await _poll_status(session_handle, op, 10.0, 0.5)
            if status != "FINISHED":
                return None
            page0 = await client.fetch_result(session_handle, op, token=0)
            results = page0.get("results") or {}
            columns: list[dict[str, Any]] = results.get("columns") or []
            status_idx = None
            for idx, col in enumerate(columns):
                try:
                    if str(col.get("name")).strip().lower() == "status":
                        status_idx = idx
                        break
                except Exception:
                    continue
            if status_idx is None:
                return None
            rows: list[dict[str, Any]] = results.get("data") or []
            if not rows:
                return None
            first = rows[0]
            fields = first.get("fields") if isinstance(first, dict) else None
            if not isinstance(fields, list) or status_idx >= len(fields):
                return None
            val = fields[status_idx]
            return val if isinstance(val, str) else str(val)
        except Exception:
            return None

    async def _submit_stop_job(
        session_handle: str, job_id: str, timeout: float = 30.0, interval: float = 0.5
    ) -> tuple[str, dict[str, Any]] | None:
        """Submit STOP JOB for a given job and poll the stop operation until terminal status.

        Returns a tuple of (status, payload) if an operation handle is available, otherwise None.
        """
        stop_exec = await client.execute_statement(
            session_handle, f"STOP JOB '{job_id}'"
        )
        stop_op = stop_exec.get("operationHandle") or stop_exec.get("operation_handle")
        if isinstance(stop_op, dict):
            stop_op = (
                stop_op.get("identifier") or stop_op.get("handle") or stop_op.get("id")
            )
        if isinstance(stop_op, str):
            return await _poll_status(session_handle, stop_op, timeout, interval)
        return None

    async def _wait_job_stopped(
        session_handle: str, job_id: str, timeout: float = 60.0, interval: float = 1.0
    ) -> tuple[bool, str | None]:
        """Wait until DESCRIBE JOB reports the job is not RUNNING (or job is gone).

        Returns (job_gone, last_status).
        """
        deadline = time.time() + timeout
        job_gone = False
        last_status: str | None = None
        while time.time() < deadline:
            last_status = await _job_status(session_handle, job_id)
            if last_status is None or str(last_status).strip().upper() != "RUNNING":
                job_gone = True
                break
            await asyncio.sleep(interval)
        return job_gone, last_status

    @server.resource("https://mcp.local/flink/info")
    async def flink_info() -> dict[str, Any]:
        """Return basic cluster information from the SQL Gateway /v1/info endpoint."""
        return await client.get_info()

    @server.tool()
    async def open_new_session(
        properties: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Open a new session and return { sessionHandle, ... }."""
        return await client.open_session(properties or {})

    @server.tool()
    async def get_config(session_handle: str) -> dict[str, Any]:
        """Return current session configuration (properties) for the given session."""
        return await client.get_session(session_handle)

    @server.tool()
    async def configure_session(session_handle: str, statement: str) -> dict[str, Any]:
        """Apply a single session-scoped DDL/config statement (CREATE/USE/SET/RESET/etc.)."""
        return await client.configure_session(
            session_handle=session_handle, statement=statement
        )

    @server.tool()
    async def run_query_collect_and_stop(
        session_handle: str,
        query: str,
        max_rows: int = 5,
        max_seconds: float = 15.0,
    ) -> dict[str, Any]:
        """Run a short-lived query, concatenate results.data until max_rows, then stop the job.

        Returns a compact payload: { "columns": [...], "data": [...] }.
        """
        deadline = time.time() + max_seconds

        try:
            exec_resp = await client.execute_statement(session_handle, query)
        except Exception as e:
            return {
                "errorType": "EXECUTE_EXCEPTION",
                "message": str(e),
            }
        op = exec_resp.get("operationHandle")
        if not isinstance(op, str):
            return {
                "errorType": "NO_OPERATION_HANDLE",
                "message": "execute returned no handle",
            }

        status, status_payload = await _poll_status(
            session_handle, op, max(0.0, deadline - time.time()), 0.5
        )
        if status != "FINISHED":
            err: dict[str, Any] = {
                "errorType": f"OPERATION_{status}",
                "message": "operation did not finish successfully",
                "status": status,
                "statusPayload": status_payload,
            }
            try:
                err_page0 = await client.fetch_result(session_handle, op, token=0)
                err["errorPage0"] = err_page0
            except Exception:
                pass
            return err

        data_accum: list[Any] = []
        columns: list[Any] | None = None
        token = 0
        jid: str | None = None

        while len(data_accum) < max_rows and time.time() < deadline:
            page = await client.fetch_result(session_handle, op, token=token)
            if jid is None:
                jid = _extract_job_id(page)
            rtype = str(page.get("resultType") or "").upper()
            if rtype == "NOT_READY":
                await asyncio.sleep(0.25)
                continue

            res = page.get("results") or {}
            if columns is None:
                cols = res.get("columns") or []
                if isinstance(cols, list) and cols:
                    columns = cols

            page_data: list[Any] = res.get("data") or []
            if isinstance(page_data, list) and page_data:
                need = max_rows - len(data_accum)
                if need > 0:
                    data_accum.extend(page_data[:need])
                # Stop polling as soon as we reached the quota
                if len(data_accum) >= max_rows:
                    break

            token += 1
            if rtype == "EOS":
                break

        if jid:
            await _submit_stop_job(session_handle, jid, 30.0, 0.5)

        try:
            await client.close_operation(session_handle, op)
        except Exception:
            pass

        return {"columns": (columns or []), "data": data_accum}

    @server.tool()
    async def run_query_stream_start(session_handle: str, query: str) -> dict[str, Any]:
        """Start a streaming query and return its cluster jobID; leaves the job running."""

        try:
            exec_resp = await client.execute_statement(session_handle, query)
        except Exception as e:
            return {
                "errorType": "EXECUTE_EXCEPTION",
                "message": str(e),
            }
        op = exec_resp.get("operationHandle")
        if not isinstance(op, str):
            return {
                "errorType": "NO_OPERATION_HANDLE",
                "message": "execute returned no handle",
            }

        status, status_payload = await _poll_status(session_handle, op, 60.0, 0.5)
        if status != "FINISHED":
            err: dict[str, Any] = {
                "errorType": f"OPERATION_{status}",
                "message": "operation did not finish successfully",
                "status": status,
                "statusPayload": status_payload,
            }
            try:
                err_page0 = await client.fetch_result(session_handle, op, token=0)
                err["errorPage0"] = err_page0
            except Exception:
                pass
            return err

        # Try to read jobID from token 0; if NOT_READY, retry a few times
        retries = 20
        jid: str | None = None
        page0: dict[str, Any] = {}
        while retries > 0:
            page0 = await client.fetch_result(session_handle, op, token=0)
            jid = _extract_job_id(page0)
            rtype = str(page0.get("resultType") or "").upper()
            if jid or rtype != "NOT_READY":
                break
            await asyncio.sleep(0.25)
            retries -= 1
        if not isinstance(jid, str):
            return {
                "errorType": "JOB_ID_NOT_AVAILABLE",
                "message": "job id not present in results",
            }

        return {"jobID": jid, "operationHandle": op}

    @server.tool()
    async def cancel_job(session_handle: str, job_id: str) -> dict[str, Any]:
        """Issue STOP JOB <job_id> and remove internal tracking state for that job."""
        logger.debug("cancel_job: submitting STOP JOB %s", job_id)
        stop_status = await _submit_stop_job(session_handle, job_id, 30.0, 0.5)
        if stop_status is not None:
            status, payload = stop_status
            logger.debug(
                "cancel_job: STOP operation status=%s payload=%s", status, payload
            )

        # Wait until job is no longer running according to DESCRIBE JOB
        logger.debug("cancel_job: waiting for job %s to stop (DESCRIBE JOB)", job_id)
        job_gone, last_status = await _wait_job_stopped(
            session_handle, job_id, 60.0, 1.0
        )
        logger.debug("cancel_job: job_gone=%s last_status=%s", job_gone, last_status)

        return {
            "jobID": job_id,
            "status": "STOP_SUBMITTED",
            "jobGone": job_gone,
            "jobStatus": last_status,
        }

    @server.tool()
    async def fetch_result_page(
        session_handle: str, operation_handle: str, token: int
    ) -> dict[str, Any]:
        """Fetch a single page for the given operation handle and token."""
        page = await client.fetch_result(session_handle, operation_handle, token=token)
        rtype = str(page.get("resultType") or "").upper()
        return {"page": page, "isEnd": rtype == "EOS", "nextToken": token + 1}

    @server.prompt()
    def manage_session() -> str:
        return (
            "If you do not have a valid 'sessionHandle', call open_new_session() first and remember the handle. "
            "If a session has expired or is invalid, create a new one and continue."
        )

    return server


def main() -> None:
    server = build_server()
    server.run(transport="stdio")


if __name__ == "__main__":
    main()
