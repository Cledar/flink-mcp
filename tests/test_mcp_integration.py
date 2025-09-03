import pytest
from fastmcp import Client

from typing import Any


async def _ensure_datagen_table(client: Client[Any], session_handle: str) -> None:
    """Create a temporary unbounded datagen table for streaming"""
    await client.call_tool(
        "configure_session",
        {
            "session_handle": session_handle,
            "statement": """
        CREATE TEMPORARY TABLE gen_stream (
          id BIGINT,
          ts TIMESTAMP_LTZ(3)
        ) WITH (
          'connector' = 'datagen',
          'rows-per-second' = '2',
          'fields.id.kind' = 'random'
        )
        """.strip(),
        },
    )


@pytest.mark.asyncio
@pytest.mark.integration
async def test_run_query_collect_and_stop_success(
    integration_client: Client[Any], integration_session_handle: str
) -> None:
    # Run query
    query_result = await integration_client.call_tool(
        "run_query_collect_and_stop",
        {
            "session_handle": integration_session_handle,
            "query": "SELECT 1",
            "max_rows": 5,
            "max_seconds": 10.0,
        },
    )

    result_data = query_result.data
    assert "errorType" not in result_data
    assert "data" in result_data


@pytest.mark.asyncio
@pytest.mark.integration
async def test_run_query_stream_start_with_datagen_then_cancel(
    integration_client: Client[Any], integration_session_handle: str
) -> None:
    # Setup datagen table
    await _ensure_datagen_table(integration_client, integration_session_handle)

    # Start streaming query
    start_result = await integration_client.call_tool(
        "run_query_stream_start",
        {
            "session_handle": integration_session_handle,
            "query": "SELECT id, ts FROM gen_stream",
        },
    )

    start_data = start_result.data

    if start_data.get("errorType") == "JOB_ID_NOT_AVAILABLE":
        pytest.skip(
            "JobId not present in results; datagen or job id surfacing not available in this env"
        )

    job_id = start_data.get("jobID")
    op = start_data.get("operationHandle")
    assert isinstance(job_id, str) and job_id
    assert isinstance(op, str) and op

    # Fetch one page
    page_result = await integration_client.call_tool(
        "fetch_result_page",
        {
            "session_handle": integration_session_handle,
            "operation_handle": op,
            "token": 1,
        },
    )
    page_data = page_result.data
    assert "page" in page_data

    # Cancel job
    cancel_result = await integration_client.call_tool(
        "cancel_job", {"session_handle": integration_session_handle, "job_id": job_id}
    )

    cancel_data = cancel_result.data
    assert cancel_data.get("jobID") == job_id
    assert cancel_data.get("jobGone") is True


@pytest.mark.asyncio
@pytest.mark.integration
async def test_run_query_collect_and_stop_error_flow(
    integration_client: Client[Any], integration_session_handle: str
) -> None:
    # Run invalid query
    query_result = await integration_client.call_tool(
        "run_query_collect_and_stop",
        {
            "session_handle": integration_session_handle,
            "query": "SELECT * FROM no_such_table",
            "max_rows": 5,
            "max_seconds": 5.0,
        },
    )

    result_data = query_result.data
    assert "errorType" in result_data
    assert (
        "status" in result_data
        or "statusPayload" in result_data
        or "errorPage0" in result_data
    )


@pytest.mark.asyncio
@pytest.mark.integration
async def test_run_query_stream_start_error_flow(
    integration_client: Client[Any], integration_session_handle: str
) -> None:
    # Run invalid streaming query
    stream_result = await integration_client.call_tool(
        "run_query_stream_start",
        {
            "session_handle": integration_session_handle,
            "query": "SELECT * FROM no_such_table",
        },
    )

    result_data = stream_result.data

    if result_data.get("errorType"):
        assert result_data["errorType"] in {
            "JOB_ID_NOT_AVAILABLE",
            "OPERATION_ERROR",
            "OPERATION_TIMEOUT",
            "OPERATION_CANCELED",
            "OPERATION_CLOSED",
        }
        if result_data["errorType"] != "JOB_ID_NOT_AVAILABLE":
            assert "statusPayload" in result_data or "errorPage0" in result_data
    else:
        # In unlikely event the environment resolves the table, ensure jobID present
        assert isinstance(result_data.get("jobID"), str)
