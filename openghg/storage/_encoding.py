"""Utilities for managing encodings of Xarray Datasets."""

from typing import Any
from collections.abc import Iterable

from ._zarr_compat import has_async_store_api


def normalise_zarr_encoding(encoding: dict) -> dict:
    """Retain legacy compressor options with xarray's Zarr 3 backend."""
    result = {name: options.copy() for name, options in encoding.items()}
    if has_async_store_api():
        for options in result.values():
            if "compressor" in options:
                compressor = options.pop("compressor")
                options.setdefault("compressors", (compressor,) if compressor is not None else None)
    return result


def get_zarr_encoding(data_vars: Iterable, compressor: Any | None = None, filters: Any | None = None) -> dict:
    """Return a dictionary of zarr compression settings for the given data.

    Args:
        data_vars: Data variables to encode
        compressor: Compressor to use, see https://zarr.readthedocs.io/en/stable/tutorial.html#compressors
        filters: Filters to use, see https://zarr.readthedocs.io/en/stable/tutorial.html#filters
    Returns:
        dict: Dictionary of encoding settings for zarr store
    """
    if compressor is None:
        return {}

    encoding = {"compressor": compressor}
    if filters is not None:
        encoding["filters"] = filters

    return normalise_zarr_encoding({var: encoding for var in data_vars})
