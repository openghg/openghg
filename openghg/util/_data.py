"""Utils for getting package data."""

from importlib import resources
from pathlib import Path
from typing import cast


def openghg_data_path() -> Path:
    """Get path to OpenGHG data folder.

    Due to a problem with importlib and editable installs, we have added a check
    to see if the data path exists, and if not, try looking within a subdirectory
    of the path returned by `importlib.resources.files`.
    """
    data_path = resources.files("openghg") / "data"

    # importlib.abc.Traversable has a .exists() method, but still getting a mypy warning...
    if not data_path.exists():  # type: ignore
        data_path = resources.files("openghg") / "openghg/data"

        if not data_path.exists():  # type: ignore
            raise RuntimeError(
                "Cannot find 'openghg/data'; this might be due to using an editable install of openghg."
            )

    with resources.as_file(data_path) as f:
        return cast(Path, f)
