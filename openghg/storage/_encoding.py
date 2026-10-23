"""Utilities for managing encodings of Xarray Datasets."""

from typing import Any
from collections.abc import Iterable

from ._zarr_compat import has_async_store_api


def normalise_zarr_encoding(encoding: dict) -> dict:
    """Normalize compressor keys without changing codec objects."""
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
        compressor: numcodecs compressor for format 2, or a native ``zarr.codecs``
            codec for format 3. Codecs are not converted between formats.
        filters: Filters compatible with the selected format.
    Returns:
        dict: Dictionary of encoding settings for zarr store
    """
    if compressor is None:
        return {}

    encoding = {"compressor": compressor}
    if filters is not None:
        encoding["filters"] = filters

    return normalise_zarr_encoding({var: encoding for var in data_vars})
