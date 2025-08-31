"""Shared test fixtures for FastMCP server testing."""

import pytest
import pytest_asyncio
from fastmcp import Client

from flink_mcp.flink_mcp_server import build_server


@pytest.fixture
def server():
    """Create a FastMCP server instance."""
    return build_server()


@pytest_asyncio.fixture
async def client(server):
    """Create a FastMCP client connected to the server."""
    async with Client(server) as client:
        yield client


@pytest_asyncio.fixture
async def session_handle(client):
    """Create a session and return the session handle."""
    session_result = await client.call_tool("open_new_session", {})
    return session_result.data["sessionHandle"]
