"""Read/write coverage for the metadata and codec conventions of each Zarr format."""

import json

from numcodecs import Blosc
import numpy as np
import pytest
import xarray as xr
import zarr

from openghg.storage import get_zarr_directory_store
from openghg.storage._zarr_compat import zarr_has_async_store_api


@pytest.mark.parametrize("zarr_format", [2, 3])
@pytest.mark.parametrize("reopen_format", [None, 2, 3])
def test_reopened_store_preserves_format_and_metadata(tmp_path, zarr_format, reopen_format):
    if zarr_format == 3 and not zarr_has_async_store_api():
        pytest.skip("Format 3 requires the Zarr 3 package")

    # Use each format's native compressor and deliberately misaligned Dask chunks.
    compressor = zarr.codecs.BloscCodec(cname="zstd", clevel=3) if zarr_format == 3 else Blosc(cname="zstd")
    original = xr.Dataset(
        {"x": ("time", np.arange(12, dtype=float))},
        coords={"time": np.arange(12)},
        attrs={"title": "metadata round trip"},
    )
    store = get_zarr_directory_store(tmp_path, zarr_format=zarr_format, compressor=compressor)
    store.insert(original.isel(time=slice(0, 6)).chunk(time=4))

    # Omitting the format or requesting another format must not convert existing data.
    kwargs = {} if reopen_format is None else {"zarr_format": reopen_format}
    reopened = get_zarr_directory_store(tmp_path, **kwargs)
    xr.testing.assert_identical(reopened.get(), original.isel(time=slice(0, 6)))
    reopened.insert(original.isel(time=slice(6, None)).chunk(time=3))
    update = original.isel(time=[1, 3, 8]).copy(deep=True)
    update["x"] += 100
    reopened.update(update.chunk(time=1))

    expected = original.copy(deep=True)
    expected["x"][dict(time=[1, 3, 8])] += 100
    xr.testing.assert_identical(get_zarr_directory_store(tmp_path).get(), expected)

    if zarr_format == 2:
        metadata = json.loads((tmp_path / ".zmetadata").read_text())["metadata"]
        assert metadata[".zgroup"]["zarr_format"] == 2
        assert metadata["x/.zarray"]["shape"] == [12]
        assert metadata["x/.zarray"]["compressor"]["id"] == "blosc"
        assert not (tmp_path / "zarr.json").exists()
    else:
        group_metadata = json.loads((tmp_path / "zarr.json").read_text())
        array_metadata = json.loads((tmp_path / "x/zarr.json").read_text())
        assert group_metadata["zarr_format"] == 3
        assert group_metadata.get("consolidated_metadata") is None
        assert array_metadata["shape"] == [12]
        assert any(codec["name"] == "blosc" for codec in array_metadata["codecs"])
        assert not (tmp_path / ".zmetadata").exists()


def test_new_store_defaults_to_consolidated_format_two(tmp_path):
    data = xr.Dataset({"x": ("time", [1.0, 2.0])}, coords={"time": [0, 1]})
    store = get_zarr_directory_store(tmp_path)
    store.insert(data)

    assert json.loads((tmp_path / ".zgroup").read_text())["zarr_format"] == 2
    assert (tmp_path / ".zmetadata").is_file()
    assert not (tmp_path / "zarr.json").exists()
    xr.testing.assert_identical(store.get(), data)
