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

### Tools (v0.2.0)

- `flink_info` (resource): returns cluster info from `/v3/info`.
- `get_config`: returns current managed session configuration (no session handle exposed).
- `configure_session(statement: str)`: apply session-scoped DDL/config (CREATE/USE/SET/RESET/LOAD/UNLOAD/ADD JAR).
- `run_query_collect_and_stop(query: str, max_rows: int=5, max_seconds: float=15.0)`: execute, fetch up to N rows within T seconds, then STOP the job if a `jobID` is present; closes the operation.
- `run_query_stream_start(query: str)`: execute a streaming query and return only `jobID`; the job is left running.
- `fetch_result_by_jobid(job_id: str)`: fetch a single page for a tracked job; returns `{ page, nextToken, isEnd }`.
- `cancel_job(job_id: str)`: issue `STOP JOB '<job_id>'`, wait until job status is not RUNNING, and clear tracking state. Returns `{ jobID, status, jobGone, jobStatus }`.

### Notes

- The server owns a single long‑lived session; tools do not expose session/operation handles.
- `run_query_stream_start` peeks token 0 to read `jobID`; use `fetch_result_by_jobid` to stream results.
- `cancel_job` issues STOP JOB; `close_operation` is invoked internally where appropriate.
- Endpoints target SQL Gateway v3-style paths.


