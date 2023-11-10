from typing import Any, Dict, Literal, Optional, Iterable


def get_zarr_encoding(
    data_vars: Iterable, compressor: Optional[Any] = None, filters: Optional[Any] = None
) -> Dict:
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


def get_chunks_footprint() -> Dict[str, int]:
    """Get chunks for a footprint Dataset over the EUROPE domain"""
    # if domain == "EUROPE":
    raise NotImplementedError("Chunks should be set for each data type by the user")
    return {"time": 12}
    # else:
    #     raise NotImplementedError(f"Domain {domain} not implemented")


def get_chunks(data_type: Literal["footprint"]) -> Dict[str, int]:
    """Get chunks for a given data type"""
    if data_type == "footprints":
        return get_chunks_footprint()
    else:
        raise NotImplementedError(f"Data type {data_type} not implemented")
