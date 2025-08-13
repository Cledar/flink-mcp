## Flink MCP Server

This project provides an MCP server that connects to Apache Flink SQL Gateway.

### Prerequisites

- A running Apache Flink cluster and SQL Gateway
  - Start cluster: `./bin/start-cluster.sh`
  - Start gateway: `./bin/sql-gateway.sh start -Dsql-gateway.endpoint.rest.address=localhost`
  - Verify: `curl http://localhost:8083/v1/info`

- Configure environment:
  - Set `SQL_GATEWAY_API_BASE_URL` (default `http://localhost:8083`). You can use a `.env` file at repo root.

### Run

Use the MCP stdio transport:

```bash
python -m ff2025
```

This starts the MCP server over stdio. Configure your MCP-compatible client to launch the module `ff2025`.

### Tools

- `open_a_new_session(properties: dict|None)` → returns `sessionHandle`
- `get_session_handle_config(session_handle: str)`
- `execute_a_query(session_handle: str, query: str, execution_config: dict|None)` → returns `operationHandle`
- `get_operation_status(session_handle: str, operation_handle: str)`
- `fetch_result_page(session_handle: str, operation_handle: str, token: int=0, row_format: str="OBJECT")`

### Notes

- Endpoints target SQL Gateway v3-style paths when available.
- Inspired by: "Hands-on MCP Server Deep Dive: Connecting Flink SQL Gateway to the LLM Ecosystem".


