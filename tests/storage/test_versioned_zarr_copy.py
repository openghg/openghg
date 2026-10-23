"""Version copying preserves encoded storage independently of append/update."""

import numpy as np
import pytest
import xarray as xr

from openghg.storage import get_versioned_zarr_directory_store, get_versioned_zarr_memory_store
from openghg.storage._zarr_compat import has_async_store_api
from openghg.util._versioning import VersionError


def store_keys(store):
    if has_async_store_api():
        from zarr.core.sync import sync

        async def collect():
            return {key async for key in store.list()}

        return sync(collect())
    return set(store)


@pytest.fixture(params=["memory", "local"])
def store(request, tmp_path):
    if request.param == "memory":
        return get_versioned_zarr_memory_store()
    return get_versioned_zarr_directory_store(tmp_path / "versions")


@pytest.fixture
def dataset():
    return xr.Dataset({"x": ("time", np.arange(6))}, coords={"time": np.arange(6)})


def test_copy_replaces_existing_version_exactly(store, dataset):
    store.create_version("v1", checkout=True)
    dataset.to_zarr(store.store, mode="w", consolidated=True, zarr_format=2, encoding={"x": {"chunks": (2,)}})
    source_keys = store_keys(store.store)
    store.create_version("v2", checkout=True)
    stale = xr.Dataset(
        {"x": ("time", np.arange(10)), "stale": ("time", np.ones(10))},
        coords={"time": np.arange(10)},
    )
    stale.to_zarr(store.store, mode="w", consolidated=True, zarr_format=2, encoding={"x": {"chunks": (2,)}})
    assert {"x/3", "stale/.zarray"} <= store_keys(store.store)

    store.checkout_version("v1")
    store.copy_to_version("v2")

    assert store.current_version == "v1"
    xr.testing.assert_identical(store.get().compute(), dataset)
    store.checkout_version("v2")
    assert store_keys(store.store) == source_keys
    xr.testing.assert_identical(store.get().compute(), dataset)


def test_failed_new_version_copy_cleans_up_and_can_retry(store, dataset, monkeypatch, tmp_path):
    store.create_version("v1", checkout=True)
    dataset.to_zarr(store.store, mode="w", consolidated=True, zarr_format=2)
    source_keys = store_keys(store.store)
    destinations = []

    def fail_after_write(source, dest):
        destinations.append(dest)
        dataset.isel(time=slice(0, 2)).to_zarr(dest, mode="w", consolidated=True, zarr_format=2)
        assert store_keys(dest)
        raise RuntimeError("copy failed")

    with monkeypatch.context() as patch:
        patch.setattr("openghg.storage._zarr_store.copy_store", fail_after_write)
        with pytest.raises(RuntimeError, match="copy failed"):
            store.create_version("v2", checkout=True, copy_current=True)

    assert store_keys(destinations[0]) == set()
    assert store.versions == ["v1"]
    assert store.current_version == "v1"
    assert store_keys(store.store) == source_keys
    xr.testing.assert_identical(store.get().compute(), dataset)
    if (tmp_path / "versions").exists():
        reopened = get_versioned_zarr_directory_store(tmp_path / "versions")
        assert reopened.versions == ["v1"]

    store.create_version("v2", checkout=True, copy_current=True)
    xr.testing.assert_identical(store.get().compute(), dataset)
    dataset.assign(x=dataset.x + 10).to_zarr(store.store, mode="w", consolidated=True, zarr_format=2)
    store.checkout_version("v1")
    xr.testing.assert_identical(store.get().compute(), dataset)


def test_copy_current_version_does_not_clear_or_copy(store, dataset, monkeypatch):
    store.create_version("v1", checkout=True)
    dataset.to_zarr(store.store, mode="w", consolidated=True, zarr_format=2)

    def unexpected_operation(*args, **kwargs):
        pytest.fail("Copying to the current version must not clear or copy its data.")

    monkeypatch.setattr("openghg.storage._zarr_store.clear_store", unexpected_operation)
    monkeypatch.setattr("openghg.storage._zarr_store.copy_store", unexpected_operation)
    store.copy_to_version("v1")
    xr.testing.assert_identical(store.get().compute(), dataset)


def test_copy_requires_selected_version(store):
    with pytest.raises(VersionError, match="No version is selected"):
        store.copy_to_version("v1")
    assert store.versions == []
