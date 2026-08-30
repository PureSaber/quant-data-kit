"""Injectable public HTTP/WebSocket transports for deterministic capture tests."""

from __future__ import annotations

import asyncio
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Protocol

from quant_data_kit.exceptions import ProviderError, ValidationError


@dataclass(frozen=True)
class HttpResponse:
    url: str
    status: int
    body: bytes


class HttpClient(Protocol):
    async def get(self, url: str, *, timeout_seconds: float) -> HttpResponse: ...


class WebSocketConnection(Protocol):
    async def send(self, payload: bytes) -> None: ...

    async def receive(self, *, timeout_seconds: float) -> bytes: ...

    async def close(self) -> None: ...


class WebSocketConnector(Protocol):
    async def connect(self, url: str, *, timeout_seconds: float) -> WebSocketConnection: ...


class UrllibHttpClient:
    """Small public GET client that preserves exact response bytes and sends no credentials."""

    async def get(self, url: str, *, timeout_seconds: float) -> HttpResponse:
        if not url.startswith("https://"):
            raise ValidationError("public snapshot HTTP transport must use https://")
        return await asyncio.to_thread(self._get, url, timeout_seconds)

    @staticmethod
    def _get(url: str, timeout_seconds: float) -> HttpResponse:
        request = urllib.request.Request(
            url,
            method="GET",
            headers={"User-Agent": "quant-data-kit-public-market-capture/0.7"},
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                body = response.read()
                status = int(getattr(response, "status", 200))
                final_url = str(response.geturl())
        except (OSError, urllib.error.URLError) as exc:
            raise ProviderError(
                f"public HTTPS snapshot failed: {type(exc).__name__}: {exc}"
            ) from exc
        if status != 200:
            raise ProviderError(f"public HTTPS snapshot returned status={status}")
        if not final_url.startswith("https://"):
            raise ProviderError("public HTTPS snapshot redirected to a non-TLS URL")
        return HttpResponse(url=final_url, status=status, body=body)


class _WebsocketsConnection:
    def __init__(self, connection: object) -> None:
        self._connection = connection

    async def send(self, payload: bytes) -> None:
        await self._connection.send(payload)  # type: ignore[attr-defined]

    async def receive(self, *, timeout_seconds: float) -> bytes:
        try:
            payload = await asyncio.wait_for(
                self._connection.recv(),  # type: ignore[attr-defined]
                timeout=timeout_seconds,
            )
        except asyncio.TimeoutError as exc:
            raise ProviderError("public WebSocket receive timed out") from exc
        if isinstance(payload, str):
            return payload.encode("utf-8")
        if isinstance(payload, bytes):
            return payload
        raise ProviderError(
            f"public WebSocket returned unsupported payload: {type(payload).__name__}"
        )

    async def close(self) -> None:
        await self._connection.close()  # type: ignore[attr-defined]


class WebsocketsConnector:
    """TLS-only adapter for the pinned websockets dependency."""

    async def connect(self, url: str, *, timeout_seconds: float) -> WebSocketConnection:
        if not url.startswith("wss://"):
            raise ValidationError("public market-data WebSocket must use wss://")
        try:
            from websockets.asyncio.client import connect

            connection = await connect(
                url,
                open_timeout=timeout_seconds,
                close_timeout=timeout_seconds,
                max_size=16 * 1024 * 1024,
                ping_interval=20,
                ping_timeout=20,
            )
        except Exception as exc:
            raise ProviderError(
                f"public WebSocket connection failed: {type(exc).__name__}: {exc}"
            ) from exc
        return _WebsocketsConnection(connection)
