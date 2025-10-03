from typing import Any
from collections.abc import Iterable


def get_zarr_encoding(data_vars: Iterable, compressor: Any | None = None, filters: Any | None = None) -> dict:
    """Return a dictionary of zarr compression settings for the given data.

    Args:
        data_vars: Data variables to encode
        compressor: Compressor to use, see https://zarr.readthedocs.io/en/stable/tutorial.html#compressors
        filters: Filters to use, see https://zarr.readthedocs.io/en/stable/tutorial.html#filters
    Returns:
        Dict: Dictionary of encoding settings for zarr store
    """
    if compressor is None:
        return {}

    encoding = {"compressor": compressor}
    if filters is not None:
        encoding["filters"] = filters

    return {var: encoding for var in data_vars}
