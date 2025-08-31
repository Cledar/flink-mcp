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
        configured_base_url = base_url or os.getenv(
            "SQL_GATEWAY_API_BASE_URL", "http://localhost:8083"
        )
        self._base_url = configured_base_url.rstrip("/")
        self._client = client or httpx.Client(timeout=timeout_seconds)

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{self._base_url}{path}"

    def get_info(self) -> Dict[str, Any]:
        """GET /v3/info, returns cluster metadata (e.g., productName, version)."""
        response = self._client.get(self._url("/v3/info"))
        response.raise_for_status()
        return response.json()

    def open_session(
        self, properties: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """POST /v3/sessions. Opens a session and returns a payload including sessionHandle."""
        payload: Dict[str, Any] = {}
        if properties:
            payload["properties"] = properties
        response = self._client.post(self._url("/v3/sessions"), json=payload or None)
        response.raise_for_status()
        return response.json()

    def get_session(self, session_handle: str) -> Dict[str, Any]:
        """GET /v3/sessions/{session}. Returns session configuration (properties)."""
        response = self._client.get(self._url(f"/v3/sessions/{session_handle}"))
        response.raise_for_status()
        return response.json()

    def configure_session(
        self,
        session_handle: str,
        statement: str,
        execution_timeout_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """POST /v3/sessions/{session}/configure-session. Applies DDL/config statements."""
        payload: Dict[str, Any] = {"statement": statement}
        if execution_timeout_ms:
            payload["executionTimeout"] = execution_timeout_ms
        response = self._client.post(
            self._url(f"/v3/sessions/{session_handle}/configure-session"), json=payload
        )
        # Some gateways may return empty body on success
        try:
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError:
            raise
        except Exception:
            return {"status": "OK"}

    def execute_statement(
        self,
        session_handle: str,
        statement: str,
        *,
        execution_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """POST /v3/sessions/{session}/statements. Returns payload with operationHandle."""
        payload: Dict[str, Any] = {"statement": statement}
        if execution_config:
            payload["executionConfig"] = execution_config
        response = self._client.post(
            self._url(f"/v3/sessions/{session_handle}/statements"),
            json=payload,
        )
        response.raise_for_status()
        return response.json()

    def get_operation_status(
        self, session_handle: str, operation_handle: str
    ) -> Dict[str, Any]:
        """GET /v3/sessions/{session}/operations/{operation}/status. Returns current status."""
        response = self._client.get(
            self._url(
                f"/v3/sessions/{session_handle}/operations/{operation_handle}/status"
            )
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
        GET /v3/sessions/{session}/operations/{operation}/result/{token}?rowFormat=JSON.
        Common response fields: resultType (NOT_READY | PAYLOAD | EOS), results, jobID (streaming).
        """
        response = self._client.get(
            self._url(
                f"/v3/sessions/{session_handle}/operations/{operation_handle}/result/{token}?rowFormat=JSON"
            )
        )
        return response.json()

    def close_operation(
        self, session_handle: str, operation_handle: str
    ) -> Dict[str, Any]:
        """DELETE /v3/sessions/{session}/operations/{operation}/close. Closes and frees resources."""
        response = self._client.delete(
            self._url(
                f"/v3/sessions/{session_handle}/operations/{operation_handle}/close"
            )
        )
        try:
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError:
            raise
        except Exception:
            return {"status": "CLOSED"}

    def close(self) -> None:
        """Close underlying HTTP client."""
        self._client.close()
