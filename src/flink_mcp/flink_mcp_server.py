from __future__ import annotations

import os
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from .flink_sql_gateway_client import FlinkSqlGatewayClient

def build_server() -> FastMCP:
    load_dotenv()

    server = FastMCP("Flink SQLGateway MCP Server")

    client = FlinkSqlGatewayClient(os.getenv("SQL_GATEWAY_API_BASE_URL"))

    @server.resource("https://mcp.local/flink/info")
    def flink_info() -> Dict[str, Any]:
        """Get Flink cluster info via SQL Gateway /v1/info."""
        return client.get_info()

    @server.tool()
    def open_new_session(properties: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Open a new SQL Gateway session. Return includes 'sessionHandle'.
        If your agent/tooling supports memory, persist the handle for reuse.
        """
        return client.open_session(properties)

    @server.tool()
    def get_session_handle_config(session_handle: str) -> Dict[str, Any]:
        """Get details/configuration for a session handle."""
        return client.get_session(session_handle)

    @server.tool()
    def execute_query(
        session_handle: str,
        query: str,
        execution_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Execute a SQL statement in the given session. Returns an object that includes 'operationHandle'.
        Use the operation handle to poll status and fetch results.
        """
        return client.execute_statement(session_handle, query, execution_config=execution_config)

    @server.tool()
    def get_operation_status(session_handle: str, operation_handle: str) -> Dict[str, Any]:
        """Get status of an operation (e.g., RUNNING/FINISHED/ERROR)."""
        return client.get_operation_status(session_handle, operation_handle)

    @server.tool()
    def fetch_result_page(
        session_handle: str,
        operation_handle: str,
        token: int = 0,
    ) -> Dict[str, Any]:
        """
        Fetch paginated results for an operation. Start with token=0 and increment until 'isEnd' or similar field indicates completion.
        """
        return client.fetch_result(session_handle, operation_handle, token=token)

    @server.prompt()
    def manage_session_handle() -> str:
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




