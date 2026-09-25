"""Regression coverage for concurrent Dask writes into stored Zarr chunks."""

import dask
import numpy as np
import xarray as xr

from openghg.storage import get_zarr_directory_store


def test_noncontiguous_regions_sharing_chunk_are_written_sequentially(tmp_path, monkeypatch):
    store = get_zarr_directory_store(tmp_path, encoding={"x": {"chunks": (12,)}})
    original = xr.Dataset({"x": ("time", np.arange(12))}, coords={"time": np.arange(12)})
    store.insert(original)
    update = original.isel(time=[1, 2, 8, 9]).copy(deep=True)
    update["x"] = update.x + 100
    update = update.chunk(time=1)

    # Check the scheduling contract as well as the result: races do not fail
    # deterministically, even when both regions modify the same stored chunk.
    to_zarr = xr.Dataset.to_zarr
    region_calls = []

    def record_write(self, *args, **kwargs):
        if isinstance(kwargs.get("region"), dict):
            region_calls.append(kwargs["region"])
            assert kwargs.get("compute", True)
        return to_zarr(self, *args, **kwargs)

    monkeypatch.setattr(xr.Dataset, "to_zarr", record_write)
    with dask.config.set(scheduler="threads", num_workers=4):
        store.update(update)

    expected = original.copy(deep=True)
    expected["x"][dict(time=[1, 2, 8, 9])] += 100
    xr.testing.assert_equal(store.get(), expected)
    assert region_calls == [{"time": slice(1, 3)}, {"time": slice(8, 10)}]
