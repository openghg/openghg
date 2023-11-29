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


# TODO - update these functions once we've tested the new zarr storage with the populate scripts
def get_chunks_footprint(high_time_resolution: bool) -> Dict[str, int]:
    """Get suggested chunks for footprint data"""
    raise NotImplementedError
    if high_time_resolution:
        return {"time": 12}
    else:
        return {"time": 144}


def get_chunks(data_type: Literal["footprint"]) -> Dict[str, int]:
    """Get chunks for a given data type"""
    raise NotImplementedError
    if data_type == "footprints":
        return get_chunks_footprint()
    else:
        raise NotImplementedError(f"Data type {data_type} not implemented")
