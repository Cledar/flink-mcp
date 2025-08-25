import os
import time

import pytest

from flink_mcp.flink_sql_gateway_client import FlinkSqlGatewayClient


def _get_base_url() -> str:
    return os.getenv("SQL_GATEWAY_API_BASE_URL", "http://localhost:8083")


@pytest.mark.integration
def test_stream_start_fetch_cancel_best_effort() -> None:
    """
    Best-effort integration test for streaming workflow:
    - submit query
    - poll to FINISHED
    - fetch token 0 to read jobID
    - if jobID present: fetch next page and STOP JOB

    Note: This test will be skipped if the query is not truly streaming and does not yield a jobID.
    To force streaming behavior in your environment, set FF_MCP_STREAM_TEST_QUERY accordingly.
    """

    base_url = _get_base_url()
    client = FlinkSqlGatewayClient(base_url=base_url)
    query = os.getenv("FF_MCP_STREAM_TEST_QUERY", "SELECT 1")
    try:
        created = client.open_session()
        session_handle = created.get("sessionHandle")
        if isinstance(session_handle, dict):
            session_handle = (
                session_handle.get("identifier")
                or session_handle.get("id")
                or session_handle.get("sessionId")
            )
        assert isinstance(session_handle, str) and session_handle

        submitted = client.execute_statement(session_handle, query)
        operation_handle = submitted.get("operationHandle")
        if isinstance(operation_handle, dict):
            operation_handle = (
                operation_handle.get("identifier")
                or operation_handle.get("id")
                or operation_handle.get("operationId")
            )
        assert isinstance(operation_handle, str) and operation_handle

        # wait until operation FINISHED/ERROR/CANCELED
        deadline = time.monotonic() + 30.0
        status_value = None
        while time.monotonic() < deadline:
            status_resp = client.get_operation_status(session_handle, operation_handle)
            # support both shapes: {"status":"FINISHED"} or {"status":{"status":"FINISHED"}}
            status_value = status_resp.get("status")
            if isinstance(status_value, dict):
                status_value = status_value.get("status")
            if status_value in {"FINISHED", "CANCELED", "ERROR"}:
                break
            time.sleep(0.25)

        assert status_value == "FINISHED"

        # Peek token 0 for jobID
        page0 = client.fetch_result(session_handle, operation_handle, token=0)
        job_id = page0.get("jobID") or page0.get("jobId")
        if not isinstance(job_id, str):
            pytest.skip("No jobID present (query likely not streaming); set FF_MCP_STREAM_TEST_QUERY to a streaming query")

        # Fetch a second page (best effort)
        _ = client.fetch_result(session_handle, operation_handle, token=1)

        # Stop the job
        stop_exec = client.execute_statement(session_handle, f"STOP JOB {job_id}")
        stop_op = stop_exec.get("operationHandle")
        if isinstance(stop_op, dict):
            stop_op = stop_op.get("identifier") or stop_op.get("id") or stop_op.get("operationId")
        assert isinstance(stop_op, str) and stop_op

        stop_deadline = time.monotonic() + 30.0
        stop_status = None
        while time.monotonic() < stop_deadline:
            st = client.get_operation_status(session_handle, stop_op)
            stop_status = st.get("status")
            if isinstance(stop_status, dict):
                stop_status = stop_status.get("status")
            if stop_status in {"FINISHED", "ERROR", "CANCELED"}:
                break
            time.sleep(0.25)

        assert stop_status in {"FINISHED", "ERROR", "CANCELED"}
    finally:
        client.close()


