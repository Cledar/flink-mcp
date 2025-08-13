from __future__ import annotations

import os
from typing import Any, Dict, Optional

import httpx


class FlinkSqlGatewayClient:
    """
    Minimal client for Apache Flink SQL Gateway REST API (v3-style endpoints).

    This client intentionally returns parsed JSON dictionaries to avoid making
    assumptions about response schemas across Flink versions.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        *,
        timeout_seconds: float = 30.0,
        client: Optional[httpx.Client] = None,
    ) -> None:
        configured_base_url = base_url or os.getenv("SQL_GATEWAY_API_BASE_URL", "http://localhost:8083")
        self._base_url = configured_base_url.rstrip("/")
        self._client = client or httpx.Client(timeout=timeout_seconds)

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{self._base_url}{path}"


    def get_info(self) -> Dict[str, Any]:
        response = self._client.get(self._url("/v1/info"))
        response.raise_for_status()
        return response.json()

    def open_session(self, properties: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if properties:
            payload["properties"] = properties
        response = self._client.post(self._url("/v3/sessions"), json=payload or None)
        response.raise_for_status()
        return response.json()

    def get_session(self, session_handle: str) -> Dict[str, Any]:
        response = self._client.get(self._url(f"/v3/sessions/{session_handle}"))
        response.raise_for_status()
        return response.json()

    def execute_statement(
        self,
        session_handle: str,
        statement: str,
        *,
        execution_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"statement": statement}
        if execution_config:
            payload["executionConfig"] = execution_config
        response = self._client.post(
            self._url(f"/v3/sessions/{session_handle}/statements"),
            json=payload,
        )
        response.raise_for_status()
        return response.json()

    def get_operation_status(self, session_handle: str, operation_handle: str) -> Dict[str, Any]:
        response = self._client.get(
            self._url(f"/v3/sessions/{session_handle}/operations/{operation_handle}/status")
        )
        response.raise_for_status()
        return response.json()

    def fetch_result(
        self,
        session_handle: str,
        operation_handle: str,
        *,
        token: int = 0,
    ) -> Dict[str, Any]:
        """
        Fetch result page for an operation. The SQL Gateway typically supports token-based pagination.
        """
        response = self._client.get(
            self._url(
                f"/v3/sessions/{session_handle}/operations/{operation_handle}/result/{token}?rowFormat=JSON"
            )
        )
        response.raise_for_status()
        return response.json()

    def close(self) -> None:
        self._client.close()



