"""Locate the compiled extension without installing it.

The repo of record is a CMake project, not a Python package, so the .pyd lands
in build/ rather than site-packages. Tests find it there instead of requiring a
pip install step that would only exist to satisfy the tests.
"""

import pathlib
import sys

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _find_extension_dir():
    for candidate in (REPO_ROOT / "build", REPO_ROOT / "out", REPO_ROOT):
        if candidate.is_dir() and any(candidate.glob("mdfeed_ext*")):
            return candidate
    return None


_EXT_DIR = _find_extension_dir()
if _EXT_DIR is not None:
    sys.path.insert(0, str(_EXT_DIR))


@pytest.fixture(scope="session")
def mdfeed():
    if _EXT_DIR is None:
        pytest.skip(
            "mdfeed_ext not built. Configure with -DBUILD_PYTHON_BINDING=ON and run nmake."
        )
    import mdfeed_ext

    return mdfeed_ext
