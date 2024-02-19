import xarray as xr
from typing import Dict, List, Optional, Union
from pathlib import Path


def open_multifile_dataset(
    filepaths: List[Union[str, Path]],
    data_type: str,
    open_mfds_kwargs: Optional[Dict] = None,
):
    """Open mutliple files as an xarray dataset. This allows the use of the `open_mfdataset` function with
    some extra checks for chunking.
    """
    # We can read in the first file to get the dimensions, check the chunking schema from the data storage object and then
    # ensure that the chunking is appropriate for the data
    raise NotImplementedError


def check_chunks(
    file_paths: list[str],
    chunk_schema: Dict,
    chunks: Optional[dict[str, int]] = None,
    time_chunk: Optional[int] = None,
    lat_chunk: Optional[int] = None,
    lon_chunk: Optional[int] = None,
    max_chunk_size: Optional[Union[int, float]] = None,
    max_chunk_by: list[str] = ["lat", "lon"],
) -> xr.Dataset:
    """Load footprints from list of file paths using xr.open_mfdataset.

    Args:
        file_paths: list of file paths as strings
        chunks: optional dictionary of chunk sizes; keys must be dims of the netCDF to be loaded
        time_chunk: optional chunk size for `time` dim
        lat_chunk: optional chunk size for `lat` dim
        lon_chunk: optional chunk size for `lon` dim
        max_chunk_size: max number of bytes in chunk of `fp` variable (assuming no compression)
        max_chunk_by: list of dimensions to scale to reduce chunksize, default = ["lat", "lon"]
        open_mfds_kwargs: keyword args to pass to xr.open_mfdataset, e.g. {'parallel': True}
    Returns:
        xr.Dataset wrapping dask arrays for given files, with specified chunks
    """
    if not isinstance(file_paths, list):
        raise ValueError("file_paths must be a list of file paths")

    with xr.open_dataset(file_paths[0]) as ds:
        dim_sizes = dict(ds.sizes)
        fp_dtype = str(ds[variable].dtype)

    if any(dim not in dim_sizes for dim in ["time", "lat", "lon", "height"]):
        raise ValueError(
            f'File {file_paths[0]} is missing one of the following dimensions: "time", "lat", "lon", "height"'
        )

    # Make the 'chunks' dict, using dim_sizes for any unspecified dims
    if chunks is None:
        chunks = dim_sizes.copy()
    else:
        chunks = dict(dim_sizes, **chunks)

    # any individually specified chunks take precedence
    if time_chunk:
        chunks["time"] = time_chunk
    if lat_chunk:
        chunks["lat"] = lat_chunk
    if lon_chunk:
        chunks["lon"] = lon_chunk

    # chunk along lat/lon to get below min size
    if max_chunk_size:
        if m := re.search(r"\d+$", fp_dtype):
            fp_dtype_bytes = int(m.group(0)) / 8
        else:
            fp_dtype_bytes = 8  # assume worst case: 64 bit

        current_chunk_size = fp_dtype_bytes * chunks["time"] * chunks["lat"] * chunks["lon"]

        if current_chunk_size > max_chunk_size:
            ratio = np.power(max_chunk_size / current_chunk_size, 1 / len(max_chunk_by))
            for dim in max_chunk_by:
                # Rescale chunks, but don't allow chunks smaller than 10
                chunks[dim] = max(int(ratio * chunks[dim]), 10)

    # chunks smaller than dims should be set after loading
    small_chunks = {}
    for k in dim_sizes:
        if chunks[k] < dim_sizes[k]:
            small_chunks[k] = chunks.pop(k)

    if small_chunks:
        result = xr.open_mfdataset(file_paths, chunks=chunks, **open_mfds_kwargs).chunk(small_chunks)
    else:
        result = xr.open_mfdataset(file_paths, chunks=chunks, **open_mfds_kwargs)

    return result
