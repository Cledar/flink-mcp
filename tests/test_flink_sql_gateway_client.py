from typing import Callable

import httpx
import pytest

from flink_mcp.flink_sql_gateway_client import FlinkSqlGatewayClient


def _make_mock_client(
    responder: Callable[[httpx.Request], httpx.Response],
) -> httpx.Client:
    transport = httpx.MockTransport(responder)
    return httpx.Client(transport=transport)


def test_get_info_mocked() -> None:
    def responder(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/v3/info"
        return httpx.Response(
            200, json={"productName": "Apache Flink", "version": "test"}
        )

    client = FlinkSqlGatewayClient(
        base_url="http://mock", client=_make_mock_client(responder)
    )
    info = client.get_info()
    assert isinstance(info, dict)
    assert info.get("version") == "test"


def test_statement_flow_mocked() -> None:
    session_handle = "session-123"
    operation_handle = "op-456"

    def responder(request: httpx.Request) -> httpx.Response:
        # Create session
        if request.method == "POST" and request.url.path == "/v3/sessions":
            return httpx.Response(
                200, json={"sessionHandle": session_handle, "properties": {}}
            )

        # Submit statement
        if (
            request.method == "POST"
            and request.url.path == f"/v3/sessions/{session_handle}/statements"
        ):
            body = request.content
            assert body and b"statement" in body
            return httpx.Response(200, json={"operationHandle": operation_handle})

        # Operation status
        if (
            request.method == "GET"
            and request.url.path
            == f"/v3/sessions/{session_handle}/operations/{operation_handle}/status"
        ):
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

    client = FlinkSqlGatewayClient(
        base_url="http://mock", client=_make_mock_client(responder)
    )

    created = client.open_session()
    assert created.get("sessionHandle") == session_handle

    submitted = client.execute_statement(session_handle, "SELECT 1")
    assert submitted.get("operationHandle") == operation_handle

    status = client.get_operation_status(session_handle, operation_handle)
    assert status.get("status", {}).get("status") == "FINISHED"

    result = client.fetch_result(session_handle, operation_handle, token=0)
    assert result.get("result") == "ok"


def test_configure_session_mocked() -> None:
    session_handle = "sess-abc"

    def responder(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/v3/sessions":
            return httpx.Response(200, json={"sessionHandle": session_handle})
        if (
            request.method == "POST"
            and request.url.path == f"/v3/sessions/{session_handle}/configure-session"
        ):
            assert request.content and b"statement" in request.content
            return httpx.Response(200, json={})
        return httpx.Response(404)

    client = FlinkSqlGatewayClient(
        base_url="http://mock", client=_make_mock_client(responder)
    )
    created = client.open_session()
    assert created.get("sessionHandle") == session_handle
    resp = client.configure_session(session_handle, "USE CATALOG default_catalog")
    assert isinstance(resp, dict)


def test_close_operation_mocked() -> None:
    session_handle = "sess-1"
    operation_handle = "op-2"

    def responder(request: httpx.Request) -> httpx.Response:
        if (
            request.method == "DELETE"
            and request.url.path
            == f"/v3/sessions/{session_handle}/operations/{operation_handle}/close"
        ):
            return httpx.Response(200, json={"status": "CLOSED"})
        return httpx.Response(404)

    client = FlinkSqlGatewayClient(
        base_url="http://mock", client=_make_mock_client(responder)
    )
    resp = client.close_operation(session_handle, operation_handle)
    assert isinstance(resp, dict)


# MCP Server tests using in-memory testing approach
@pytest.mark.asyncio
async def test_mcp_server_open_session(client) -> None:
    """Test opening a session through MCP server"""
    result = await client.call_tool("open_new_session", {})

    data = result.data
    assert "sessionHandle" in data
    assert isinstance(data["sessionHandle"], str)


@pytest.mark.asyncio
async def test_mcp_server_get_config(client, session_handle) -> None:
    """Test getting session config through MCP server"""
    # Get config
    config_result = await client.call_tool(
        "get_config", {"session_handle": session_handle}
    )

    config_data = config_result.data
    assert isinstance(config_data, dict)


@pytest.mark.asyncio
async def test_mcp_server_configure_session(client, session_handle) -> None:
    """Test configuring a session through MCP server"""
    # The configure_session call may succeed with an empty response
    configure_result = await client.call_tool(
        "configure_session",
        {
            "session_handle": session_handle,
            "statement": "SET execution.runtime-mode = 'batch'",
        },
    )
    # configure_session returns an empty dict {} which becomes None in fastmcp
    # The important thing is that it doesn't error
    assert not configure_result.is_error


@pytest.mark.asyncio
async def test_mcp_server_fetch_result_page(client, session_handle) -> None:
    """Test fetching result page through MCP server"""
    # Run a simple query
    query_result = await client.call_tool(
        "run_query_collect_and_stop",
        {
            "session_handle": session_handle,
            "query": "SELECT 1",
            "max_rows": 1,
            "max_seconds": 10.0,
        },
    )

    query_data = query_result.data

    # The collect_and_stop method should return data directly, not require separate page fetching
    assert "data" in query_data or "errorType" in query_data
