"""Shared test fixtures and settings for FastMCP server testing.

This config also skips tests marked "integration" unless explicitly enabled.
"""

import os

import httpx
import pytest
import pytest_asyncio
from fastmcp import Client

from flink_mcp.flink_mcp_server import build_server


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run integration tests that require a live Flink SQL Gateway.",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    run_integration = config.getoption("--run-integration") or os.getenv(
        "RUN_INTEGRATION_TESTS"
    ) in {"1", "true", "True"}
    skip_integration = pytest.mark.skip(
        reason="integration tests disabled (use --run-integration)"
    )
    for item in items:
        if "integration" in item.keywords and not run_integration:
            item.add_marker(skip_integration)


def _make_mock_transport() -> httpx.MockTransport:
    """Provide a minimal mocked Flink SQL Gateway API for non-integration tests."""
    session_handle = "sess-mock"
    op_exec = "op-exec"
    op_stop = "op-stop"

    def responder(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        method = request.method

        # Open session
        if method == "POST" and path == "/v3/sessions":
            return httpx.Response(
                200, json={"sessionHandle": session_handle, "properties": {}}
            )

        # Get session
        if method == "GET" and path == f"/v3/sessions/{session_handle}":
            return httpx.Response(200, json={"properties": {}})

        # Configure session
        if (
            method == "POST"
            and path == f"/v3/sessions/{session_handle}/configure-session"
        ):
            return httpx.Response(200, json={})

        # Execute statements (regular or STOP JOB)
        if method == "POST" and path == f"/v3/sessions/{session_handle}/statements":
            body = (request.content or b"").decode("utf-8", errors="ignore").lower()
            if "stop job" in body:
                return httpx.Response(200, json={"operationHandle": op_stop})
            return httpx.Response(200, json={"operationHandle": op_exec})

        # Operation status
        if (
            method == "GET"
            and path == f"/v3/sessions/{session_handle}/operations/{op_exec}/status"
        ):
            return httpx.Response(200, json={"status": "FINISHED"})
        if (
            method == "GET"
            and path == f"/v3/sessions/{session_handle}/operations/{op_stop}/status"
        ):
            return httpx.Response(200, json={"status": "FINISHED"})

        # Fetch results
        if (
            method == "GET"
            and path == f"/v3/sessions/{session_handle}/operations/{op_exec}/result/0"
        ):
            return httpx.Response(
                200,
                json={
                    "resultType": "PAYLOAD",
                    "results": {
                        "columns": [{"name": "col1"}],
                        "data": [{"fields": [1]}],
                    },
                    "jobID": "job-1",
                },
            )
        if (
            method == "GET"
            and path == f"/v3/sessions/{session_handle}/operations/{op_exec}/result/1"
        ):
            return httpx.Response(
                200,
                json={
                    "resultType": "EOS",
                    "results": {
                        "columns": [{"name": "col1"}],
                        "data": [],
                    },
                },
            )

        # Close operation
        if (
            method == "DELETE"
            and path == f"/v3/sessions/{session_handle}/operations/{op_exec}/close"
        ):
            return httpx.Response(200, json={"status": "CLOSED"})

        # Default: not mocked
        return httpx.Response(404, json={"message": "not mocked", "path": path})

    return httpx.MockTransport(responder)


@pytest_asyncio.fixture
async def client():
    """Create a FastMCP client connected to a mocked server."""
    transport = _make_mock_transport()
    http_client = httpx.AsyncClient(transport=transport)
    server = build_server(base_url="http://mock", http_client=http_client)
    async with Client(server) as client:
        yield client


@pytest_asyncio.fixture
async def session_handle(client):
    """Create a session and return the session handle."""
    session_result = await client.call_tool("open_new_session", {})
    return session_result.data["sessionHandle"]


@pytest_asyncio.fixture
async def integration_client():
    """Client connected to the live server (integration)."""
    server = build_server()
    async with Client(server) as c:
        yield c


@pytest_asyncio.fixture
async def integration_session_handle(integration_client):
    """Create a session on the live server and return its handle."""
    session_result = await integration_client.call_tool("open_new_session", {})
    return session_result.data["sessionHandle"]
