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
        assert request.url.path == "/v3/info"
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


# Integration tests moved to dedicated files:
# - tests/test_stream_workflow_integration.py
# - tests/test_mcp_error_flows_integration.py


def test_configure_session_mocked() -> None:
    session_handle = "sess-abc"

    def responder(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/v3/sessions":
            return httpx.Response(200, json={"sessionHandle": session_handle})
        if request.method == "POST" and request.url.path == f"/v3/sessions/{session_handle}/configure-session":
            assert request.content and b"statement" in request.content
            return httpx.Response(200, json={})
        return httpx.Response(404)

    client = FlinkSqlGatewayClient(base_url="http://mock", client=_make_mock_client(responder))
    created = client.open_session()
    assert created.get("sessionHandle") == session_handle
    resp = client.configure_session(session_handle, "USE CATALOG default_catalog")
    assert isinstance(resp, dict)


def test_close_operation_mocked() -> None:
    session_handle = "sess-1"
    operation_handle = "op-2"

    def responder(request: httpx.Request) -> httpx.Response:
        if request.method == "DELETE" and request.url.path == f"/v3/sessions/{session_handle}/operations/{operation_handle}/close":
            return httpx.Response(200, json={"status": "CLOSED"})
        return httpx.Response(404)

    client = FlinkSqlGatewayClient(base_url="http://mock", client=_make_mock_client(responder))
    resp = client.close_operation(session_handle, operation_handle)
    assert isinstance(resp, dict)


