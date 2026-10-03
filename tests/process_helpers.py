"""Process crash injection without Python, CRT or DLL cleanup."""

from __future__ import annotations

import os
from typing import NoReturn


def hard_exit(code: int) -> NoReturn:
    """Kill the current worker at the fault point, preserving the exact exit code."""
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        # Windows CRT _exit can run DLL_PROCESS_DETACH callbacks. DuckDB can
        # fault there after its other threads are gone, replacing our exit code.
        # TerminateProcess models a hard crash without that unrelated teardown.
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.GetCurrentProcess.restype = wintypes.HANDLE
        kernel.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
        kernel.TerminateProcess.restype = wintypes.BOOL
        if not kernel.TerminateProcess(kernel.GetCurrentProcess(), code):
            raise ctypes.WinError(ctypes.get_last_error())
        raise RuntimeError("TerminateProcess unexpectedly returned")
    os._exit(code)
