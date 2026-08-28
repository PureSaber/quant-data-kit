"""Cross-process advisory file locks with crash-safe operating-system ownership."""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from typing import BinaryIO

from quant_data_kit.exceptions import ValidationError


def _acquire_windows_lock(
    file_descriptor: int,
    path: Path,
    deadline: float,
    *,
    locking: Callable[[int, int, int], None],
    nonblocking_mode: int,
) -> None:
    """Run the Windows non-blocking retry state machine with an injected syscall."""
    while True:
        try:
            locking(file_descriptor, nonblocking_mode, 1)
            return
        except OSError as exc:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out acquiring process lock: {path}") from exc
            time.sleep(0.01)


def _release_windows_lock(
    file_descriptor: int,
    *,
    locking: Callable[[int, int, int], None],
    unlock_mode: int,
) -> None:
    locking(file_descriptor, unlock_mode, 1)


def _acquire_posix_lock(
    file_descriptor: int,
    path: Path,
    deadline: float,
    *,
    flock: Callable[[int, int], None],
    exclusive_nonblocking_operation: int,
) -> None:
    """Run the POSIX non-blocking retry state machine with an injected syscall."""
    while True:
        try:
            flock(file_descriptor, exclusive_nonblocking_operation)
            return
        except BlockingIOError as exc:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out acquiring process lock: {path}") from exc
            time.sleep(0.01)


def _release_posix_lock(
    file_descriptor: int,
    *,
    flock: Callable[[int, int], None],
    unlock_operation: int,
) -> None:
    flock(file_descriptor, unlock_operation)


@contextmanager
def _windows_file_lock(stream: BinaryIO, path: Path, deadline: float) -> Iterator[None]:
    import msvcrt

    _acquire_windows_lock(
        stream.fileno(),
        path,
        deadline,
        locking=msvcrt.locking,
        nonblocking_mode=msvcrt.LK_NBLCK,
    )
    try:
        yield
    finally:
        stream.seek(0)
        _release_windows_lock(
            stream.fileno(),
            locking=msvcrt.locking,
            unlock_mode=msvcrt.LK_UNLCK,
        )


@contextmanager
def _posix_file_lock(stream: BinaryIO, path: Path, deadline: float) -> Iterator[None]:
    import fcntl

    _acquire_posix_lock(
        stream.fileno(),
        path,
        deadline,
        flock=fcntl.flock,
        exclusive_nonblocking_operation=fcntl.LOCK_EX | fcntl.LOCK_NB,
    )
    try:
        yield
    finally:
        _release_posix_lock(
            stream.fileno(),
            flock=fcntl.flock,
            unlock_operation=fcntl.LOCK_UN,
        )


_PLATFORM_LOCK_BACKENDS = {
    "nt": _windows_file_lock,
    "posix": _posix_file_lock,
}


def _platform_lock_backend(
    platform_name: str,
) -> Callable[[BinaryIO, Path, float], AbstractContextManager[None]]:
    return _PLATFORM_LOCK_BACKENDS.get(platform_name, _posix_file_lock)


@contextmanager
def process_file_lock(path: Path, *, timeout_seconds: float = 60.0) -> Iterator[None]:
    """Lock the first byte of a stable file; process exit releases ownership automatically."""
    if timeout_seconds <= 0:
        raise ValidationError("lock timeout_seconds must be positive")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as stream:
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"\0")
            stream.flush()
            os.fsync(stream.fileno())
        stream.seek(0)
        deadline = time.monotonic() + timeout_seconds
        backend = _platform_lock_backend(os.name)
        with backend(stream, path, deadline):
            yield
