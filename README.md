## flink-mcp — Flink MCP Server

This project provides an MCP server that connects to Apache Flink SQL Gateway.

### Prerequisites

- A running Apache Flink cluster and SQL Gateway
  - Start cluster: `./bin/start-cluster.sh`
  - Start gateway: `./bin/sql-gateway.sh start -Dsql-gateway.endpoint.rest.address=localhost`
  - Verify: `curl http://localhost:8083/v3/info`

- Configure environment:
  - Set `SQL_GATEWAY_API_BASE_URL` (default `http://localhost:8083`). You can use a `.env` file at repo root.

### Run

Install and run via the console script:

```bash
pip install -e .
flink-mcp
```

MCP clients should launch the server over stdio with command: `flink-mcp`.

Ensure `SQL_GATEWAY_API_BASE_URL` is set in your environment or `.env`.


### Tools (v0.2.5)

- `flink_info` (resource): returns cluster info from `/v3/info`.
- `open_new_session(properties?: dict)` -> `{ sessionHandle, ... }`.
- `get_config(sessionHandle: str)`: returns session configuration.
- `configure_session(sessionHandle: str, statement: str)`: apply session-scoped DDL/config (CREATE/USE/SET/RESET/LOAD/UNLOAD/ADD JAR).
- `run_query_collect_and_stop(sessionHandle: str, query: str, max_rows: int=5, max_seconds: float=15.0)`: execute, fetch up to N rows within T seconds, then STOP the job if a `jobID` is present; closes the operation.
- `run_query_stream_start(sessionHandle: str, query: str)`: execute a streaming query and return `{ jobID, operationHandle }`; the job is left running.
- `fetch_result_page(sessionHandle: str, operationHandle: str, token: int)`: fetch a single page; returns `{ page, nextToken, isEnd }`.
- `cancel_job(sessionHandle: str, jobId: str)`: issue `STOP JOB '<jobId>'`, wait until DESCRIBE JOB status is not RUNNING; returns `{ jobID, status, jobGone, jobStatus }`.

### Notes

- Tools are stateless; clients manage and pass session/operation handles explicitly.
- `run_query_stream_start` returns both `jobID` and `operationHandle`; use `fetch_result_page` to stream results.
- `cancel_job` issues STOP and waits using DESCRIBE JOB; `close_operation` is invoked internally where appropriate.

- Endpoints target SQL Gateway v3-style paths.


