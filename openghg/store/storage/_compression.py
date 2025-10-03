from pathlib import Path
from typing import Any
import time
import zarr
import numcodecs
import itertools
import tempfile
from numcodecs import Blosc
import xarray as xr

from openghg.store.storage import get_zarr_encoding


def compare_compression(filepath: Path) -> dict:
    """This compresses the given dataset to a temporary zarr ZipStore
    to test the compression ratio. A dictionary containing the final size of
    the data, the compression ratio and the time taken for the compression
    is returned"

    Args:
        filepath: Path to NetCDF file
    Returns:
        dict: Dictionary of compression information
    """
    codecs = numcodecs.blosc.list_compressors()
    compression_levels = [1, 3, 5]
    # These are just ints
    shuffles = [Blosc.NOSHUFFLE, Blosc.SHUFFLE, Blosc.BITSHUFFLE]
    codec_combinations = sorted(set(list(itertools.product(codecs, compression_levels, shuffles))))

    filepath = Path(filepath)
    orig_size = filepath.stat().st_size
    compression_info: dict[str, Any] = {"original_filesize": orig_size}
    print(f"Original file size: {orig_size}")

    best_ratio = 0.0
    shortest_time = 1.0e9
    quickest_codec = ""
    best_compression_codec = ""

    with tempfile.TemporaryDirectory() as tmpdir:
        for codec, comp_level, shuffle in codec_combinations:
            with xr.open_dataset(filepath) as ds:
                codec_str = f"blosc_codec_{codec}_complevel_{comp_level}_shuffle_{shuffle}"

                zarr_path = Path(tmpdir, f"zarr_store_{codec_str}.zip")
                store = zarr.storage.ZipStore(zarr_path)

                compressor = Blosc(cname=codec, clevel=comp_level, shuffle=shuffle)
                encoding = get_zarr_encoding(data_vars=ds.data_vars, compressor=compressor)

                start = time.perf_counter()
                ds.to_zarr(store=store, encoding=encoding)
                time_taken = time.perf_counter() - start

                compressed_size = zarr_path.stat().st_size

                ratio = orig_size / compressed_size
                print(f"Finished {codec_str}")

                compression_info[codec_str] = {
                    "ratio": ratio,
                    "compressed_size_bytes": compressed_size,
                    "time_taken": time_taken,
                }

                if ratio > best_ratio:
                    best_ratio = ratio
                    best_compression_codec = codec_str

                if time_taken < shortest_time:
                    shortest_time = time_taken
                    quickest_codec = codec_str

    compression_info["best_compression_codec"] = best_compression_codec
    compression_info["quickest_codec"] = quickest_codec

    return compression_info
