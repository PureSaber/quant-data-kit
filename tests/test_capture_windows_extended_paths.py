from pathlib import Path
from types import SimpleNamespace

import pytest

from quant_data_kit.capture_v2 import storage


@pytest.mark.parametrize("anchor", ["D:\\", "\\\\?\\D:\\"])
def test_local_drive_paths_open_the_same_physical_volume(anchor, monkeypatch):
    from ctypes import wintypes

    class ResolvedPath:
        def __init__(self, value):
            self.anchor = anchor

        def resolve(self, *, strict):
            assert strict
            return self

    class OpenVolume:
        def __call__(self, device, *_args):
            assert device == "\\\\.\\D:"
            # An OS denial still fails closed after resolving either spelling.
            return wintypes.HANDLE(-1).value

    monkeypatch.setattr(storage, "Path", ResolvedPath)
    monkeypatch.setattr(
        storage.ctypes,
        "windll",
        SimpleNamespace(kernel32=SimpleNamespace(CreateFileW=OpenVolume())),
        raising=False,
    )
    with pytest.raises(storage.CapturePausedError, match="cannot open volume"):
        storage._windows_volume_identity(Path("unused"))


@pytest.mark.parametrize(
    "anchor", ["\\\\server\\share\\", "\\\\?\\UNC\\server\\share\\", "\\\\?\\Volume{abc}\\", "1:\\"]
)
def test_unsupported_windows_anchors_do_not_reach_device_probe(anchor, monkeypatch):
    class ResolvedPath:
        def __init__(self, value):
            self.anchor = anchor

        def resolve(self, *, strict):
            return self

    monkeypatch.setattr(storage, "Path", ResolvedPath)
    with pytest.raises(storage.CapturePausedError, match="cannot resolve Windows volume anchor"):
        storage._windows_volume_identity(Path("unused"))
