"""Utilities for managing encodings of Xarray Datasets."""

from typing import Any
from collections.abc import Iterable


def get_zarr_encoding(
    data_vars: Iterable, compressor: Any | None = None, filters: Any | None = None, *, zarr_format: int = 2
) -> dict:
    """Return a dictionary of zarr compression settings for the given data.

    Args:
        data_vars: Data variables to encode
        compressor: Compressor to use, see https://zarr.readthedocs.io/en/stable/tutorial.html#compressors
        filters: Filters to use, see https://zarr.readthedocs.io/en/stable/tutorial.html#filters
        zarr_format: Storage format, 2 or 3. Format 3 requires a native Zarr codec.
    Returns:
        dict: Dictionary of encoding settings for zarr store
    """
    if compressor is None:
        return {}

    encoding = {"compressors": [compressor]} if zarr_format == 3 else {"compressor": compressor}
    if filters is not None:
        encoding["filters"] = filters

    return {var: encoding for var in data_vars}
