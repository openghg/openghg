"""Regression coverage for concurrent Dask writes into stored Zarr chunks."""

import dask
import numpy as np
import pytest
import xarray as xr

from openghg.storage import get_zarr_directory_store
from openghg.storage._zarr_compat import zarr_has_async_store_api


@pytest.fixture(params=(2, 3) if zarr_has_async_store_api() else (2,), ids=lambda value: f"format{value}")
def zarr_format(request):
    return request.param


def test_noncontiguous_regions_sharing_chunk_are_written_sequentially(tmp_path, monkeypatch, zarr_format):
    store = get_zarr_directory_store(tmp_path, zarr_format=zarr_format, encoding={"x": {"chunks": (12,)}})
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


def test_misaligned_dask_insert_append_and_update(tmp_path, zarr_format):
    store = get_zarr_directory_store(tmp_path, zarr_format=zarr_format, encoding={"x": {"chunks": (4,)}})
    original = xr.Dataset({"x": ("time", np.arange(15))}, coords={"time": np.arange(15)})

    with dask.config.set(scheduler="threads", num_workers=4):
        store.insert(original.isel(time=slice(0, 6)).chunk(time=3))
        # The append begins inside a stored chunk and the Dask chunks do not
        # align with the existing grid.
        store.insert(original.isel(time=slice(6, None)).chunk(time=3))
        update = original.isel(time=slice(2, 12)).copy(deep=True)
        update["x"] += 100
        store.update(update.chunk(time=3))

    expected = original.copy(deep=True)
    expected["x"][dict(time=slice(2, 12))] += 100
    xr.testing.assert_equal(store.get(), expected)


def test_disabled_alignment_keeps_chunk_safety_validation(tmp_path, zarr_format):
    store = get_zarr_directory_store(
        tmp_path, zarr_format=zarr_format, encoding={"x": {"chunks": (4,)}}, align_chunks=False
    )
    data = xr.Dataset({"x": ("time", np.arange(12))}, coords={"time": np.arange(12)}).chunk(time=3)
    with pytest.raises(ValueError, match="overlap multiple Dask chunks"):
        store.insert(data)


def test_older_xarray_uses_validation_and_rejects_explicit_alignment(tmp_path, monkeypatch, zarr_format):
    to_zarr = xr.Dataset.to_zarr

    # Model the pre-align_chunks API while still exercising real chunk validation.
    def old_to_zarr(self, *args, **kwargs):
        assert "align_chunks" not in kwargs
        return to_zarr(self, *args, **kwargs)

    monkeypatch.setattr(xr.Dataset, "to_zarr", old_to_zarr)
    store = get_zarr_directory_store(tmp_path, zarr_format=zarr_format, encoding={"x": {"chunks": (4,)}})
    data = xr.Dataset({"x": ("time", np.arange(12))}, coords={"time": np.arange(12)})
    with pytest.raises(ValueError, match="overlap multiple Dask chunks"):
        store.insert(data.chunk(time=3))
    with pytest.raises(ValueError, match="newer Xarray"):
        get_zarr_directory_store(tmp_path, zarr_format=zarr_format, align_chunks=True)
