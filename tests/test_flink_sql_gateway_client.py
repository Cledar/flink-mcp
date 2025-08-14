import os
import time
from typing import Callable

import httpx
import pytest

from flink_mcp.flink_sql_gateway_client import FlinkSqlGatewayClient


# ----------------------
# Unit tests (mocked IO)
# ----------------------

def _make_mock_client(responder: Callable[[httpx.Request], httpx.Response]) -> httpx.Client:
    transport = httpx.MockTransport(responder)
    return httpx.Client(transport=transport)


def test_get_info_mocked() -> None:
    def responder(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/v1/info"
        return httpx.Response(200, json={"productName": "Apache Flink", "version": "test"})

    client = FlinkSqlGatewayClient(base_url="http://mock", client=_make_mock_client(responder))
    info = client.get_info()
    assert isinstance(info, dict)
    assert info.get("version") == "test"


def test_statement_flow_mocked() -> None:
    session_handle = "session-123"
    operation_handle = "op-456"

    def responder(request: httpx.Request) -> httpx.Response:
        # Create session
        if request.method == "POST" and request.url.path == "/v3/sessions":
            return httpx.Response(200, json={"sessionHandle": session_handle, "properties": {}})

        # Submit statement
        if request.method == "POST" and request.url.path == f"/v3/sessions/{session_handle}/statements":
            body = request.content
            assert body and b"statement" in body
            return httpx.Response(200, json={"operationHandle": operation_handle})

        # Operation status
        if request.method == "GET" and request.url.path == f"/v3/sessions/{session_handle}/operations/{operation_handle}/status":
            return httpx.Response(200, json={"status": {"status": "FINISHED"}})

        # Fetch result page 0
        if (
            request.method == "GET"
            and request.url.path
            == f"/v3/sessions/{session_handle}/operations/{operation_handle}/result/0"
        ):
            # Optionally ensure rowFormat=JSON is requested
            assert b"rowFormat=JSON" in (request.url.query or b"")
            return httpx.Response(200, json={"result": "ok", "data": [[1]]})

        return httpx.Response(404, json={"message": "not mocked"})

    client = FlinkSqlGatewayClient(base_url="http://mock", client=_make_mock_client(responder))

    created = client.open_session()
    assert created.get("sessionHandle") == session_handle

    submitted = client.execute_statement(session_handle, "SELECT 1")
    assert submitted.get("operationHandle") == operation_handle

    status = client.get_operation_status(session_handle, operation_handle)
    assert status.get("status", {}).get("status") == "FINISHED"

    result = client.fetch_result(session_handle, operation_handle, token=0)
    assert result.get("result") == "ok"


# ---------------------------------
# Live integration tests (real HTTP)
# ---------------------------------
def _get_base_url() -> str:
    return os.getenv("SQL_GATEWAY_API_BASE_URL", "http://localhost:8083")


@pytest.mark.integration
def test_live_info() -> None:
    base_url = _get_base_url()
    client = FlinkSqlGatewayClient(base_url=base_url)
    try:
        info = client.get_info()
        assert isinstance(info, dict)
        assert "version" in info  # Flink typically exposes version here
    finally:
        client.close()


@pytest.mark.integration
def test_live_open_session_minimal() -> None:
    base_url = _get_base_url()
    client = FlinkSqlGatewayClient(base_url=base_url)
    try:
        created = client.open_session()
        assert isinstance(created, dict)
        # Be lenient about schema across versions; just require a non-empty dict
        assert len(created) > 0
    finally:
        client.close()


@pytest.mark.integration
def test_live_select_one() -> None:
    base_url = _get_base_url()
    client = FlinkSqlGatewayClient(base_url=base_url)
    try:
        created = client.open_session()
        assert isinstance(created, dict) and len(created) > 0
        session_handle = created.get("sessionHandle")
        if isinstance(session_handle, dict):
            session_handle = session_handle.get("identifier") or session_handle.get("id") or session_handle.get("sessionId")
        assert isinstance(session_handle, str) and session_handle

        submitted = client.execute_statement(session_handle, "SELECT 1")
        operation_handle = submitted.get("operationHandle")
        if isinstance(operation_handle, dict):
            operation_handle = operation_handle.get("identifier") or operation_handle.get("id") or operation_handle.get("operationId")
        assert isinstance(operation_handle, str) and operation_handle

        deadline = time.monotonic() + 15.0
        status_value = None
        while time.monotonic() < deadline:
            status_resp = client.get_operation_status(session_handle, operation_handle)
            status_value = status_resp["status"]
            if status_value in {"FINISHED", "CANCELED", "FAILED"}:
                break
            time.sleep(0.2)

        assert status_value == "FINISHED"

        # Try first three pages quickly; SELECT 1 should appear on the first or second page
        rows = []
        for token in (0, 1, 2):
            resp = client.fetch_result(session_handle, operation_handle, token=token)
            data = resp["results"]["data"]
            if data:
                rows = data
                break
            time.sleep(0.1)

        assert rows, "No rows returned from result pages 0..2"
        first = rows[0]
        fields = first["fields"]
        assert fields[0] == 1
    finally:
        client.close()


