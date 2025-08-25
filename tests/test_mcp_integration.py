import pytest

from flink_mcp.flink_mcp_server import build_server


@pytest.mark.integration
def test_run_query_collect_and_stop_success() -> None:
    server = build_server()
    res = server.run_query_collect_and_stop(
        query="SELECT 1",
        max_rows=5,
        max_seconds=10.0,
    )
    assert "errorType" not in res
    assert "pages" in res


def _ensure_datagen_table(server) -> None:
    # Create a temporary unbounded datagen table for streaming
    server.configure_session(
        """
        CREATE TEMPORARY TABLE gen_stream (
          id BIGINT,
          ts TIMESTAMP_LTZ(3)
        ) WITH (
          'connector' = 'datagen',
          'rows-per-second' = '2',
          'fields.id.kind' = 'random'
        )
        """.strip()
    )


@pytest.mark.integration
def test_run_query_stream_start_with_datagen_then_cancel() -> None:
    server = build_server()
    _ensure_datagen_table(server)

    start = server.run_query_stream_start("SELECT id, ts FROM gen_stream")
    if start.get("errorType") == "JOB_ID_NOT_AVAILABLE":
        pytest.skip("JobId not present in results; datagen or job id surfacing not available in this env")
    job_id = start.get("jobID")
    assert isinstance(job_id, str) and job_id

    # Fetch one page
    page_res = server.fetch_result_by_jobid(job_id)
    assert "page" in page_res

    # Cancel job
    cancel = server.cancel_job(job_id)
    assert cancel.get("jobID") == job_id
    assert cancel.get("jobGone") is True


@pytest.mark.integration
def test_run_query_collect_and_stop_error_flow() -> None:
    server = build_server()
    res = server.run_query_collect_and_stop(query="SELECT * FROM no_such_table", max_rows=5, max_seconds=5.0)
    assert "errorType" in res
    assert "status" in res or "statusPayload" in res or "errorPage0" in res


@pytest.mark.integration
def test_run_query_stream_start_error_flow() -> None:
    server = build_server()
    res = server.run_query_stream_start("SELECT * FROM no_such_table")
    if res.get("errorType"):
        assert res["errorType"] in {"JOB_ID_NOT_AVAILABLE", "OPERATION_ERROR", "OPERATION_TIMEOUT", "OPERATION_CANCELED", "OPERATION_CLOSED"}
        if res["errorType"] != "JOB_ID_NOT_AVAILABLE":
            assert "statusPayload" in res or "errorPage0" in res
    else:
        # In unlikely event the environment resolves the table, ensure jobID present
        assert isinstance(res.get("jobID"), str)


