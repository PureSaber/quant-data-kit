"""Exercise deep lake partitions on Windows without a machine-wide registry change."""

import os
from pathlib import Path

import pytest


@pytest.fixture
def tmp_path(tmp_path: Path) -> Path:
    if os.name == "nt":
        path = str(tmp_path.resolve())
        if not path.startswith("\\\\?\\"):
            return Path("\\\\?\\UNC\\" + path[2:] if path.startswith("\\\\") else "\\\\?\\" + path)
    return tmp_path
