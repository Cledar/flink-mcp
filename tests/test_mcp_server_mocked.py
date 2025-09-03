import pytest
from unittest.mock import Mock

pytestmark = [pytest.mark.asyncio, pytest.mark.unit]


async def test_mcp_server_open_session(client: Mock) -> None:
    """Open a session through MCP server (mocked backend)."""
    result = await client.call_tool("open_new_session", {})
    data = result.data
    assert "sessionHandle" in data and isinstance(data["sessionHandle"], str)


async def test_mcp_server_get_config(client: Mock, session_handle: str) -> None:
    """Get session config through MCP server (mocked backend)."""
    config_result = await client.call_tool(
        "get_config", {"session_handle": session_handle}
    )
    assert isinstance(config_result.data, dict)


async def test_mcp_server_configure_session(client: Mock, session_handle: str) -> None:
    """Configure a session through MCP server (mocked backend)."""
    res = await client.call_tool(
        "configure_session",
        {
            "session_handle": session_handle,
            "statement": "SET execution.runtime-mode = 'batch'",
        },
    )
    assert not res.is_error


async def test_mcp_server_fetch_result_page(client: Mock, session_handle: str) -> None:
    """Fetch two pages and assert pagination contract via MCP server (mocked backend)."""
    start = await client.call_tool(
        "run_query_stream_start",
        {"session_handle": session_handle, "query": "SELECT 1"},
    )
    op = start.data.get("operationHandle")
    assert isinstance(op, str) and op

    page0 = await client.call_tool(
        "fetch_result_page",
        {"session_handle": session_handle, "operation_handle": op, "token": 0},
    )
    p0 = page0.data
    assert p0.get("isEnd") is False
    assert p0.get("nextToken") == 1
    assert "page" in p0 and isinstance(p0["page"], dict)

    page1 = await client.call_tool(
        "fetch_result_page",
        {"session_handle": session_handle, "operation_handle": op, "token": 1},
    )
    p1 = page1.data
    assert p1.get("isEnd") is True
    assert p1.get("nextToken") == 2
