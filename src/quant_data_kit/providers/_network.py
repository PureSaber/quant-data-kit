"""Network configuration for AKShare providers."""

from __future__ import annotations


def configure_network() -> None:
    """Prefer direct connections when system proxy is unavailable."""
    import os

    os.environ.setdefault("NO_PROXY", "*")
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy"):
        os.environ.pop(key, None)
