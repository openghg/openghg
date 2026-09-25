"""Test version copying independently of the OpenGHG append/update writers."""

import numpy as np
import pytest
import xarray as xr

from openghg.storage import get_versioned_zarr_directory_store, get_versioned_zarr_memory_store
from openghg.storage._zarr_compat import iter_store_keys
from openghg.storage._zarr_copy import copy_zarr_store
from openghg.util._versioning import VersionError


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
    dataset.to_zarr(store.store, mode="w", consolidated=True, zarr_format=2)
    source_keys = set(iter_store_keys(store.store))
    store.create_version("v2", checkout=True)
    dataset.assign(stale=("time", np.ones(6))).to_zarr(
        store.store, mode="w", consolidated=True, zarr_format=2
    )
    assert any(key.startswith("stale/") for key in iter_store_keys(store.store))

    store.checkout_version("v1")
    store.copy_to_version("v2")

    assert store.current_version == "v1"
    xr.testing.assert_identical(store.get().compute(), dataset)
    store.checkout_version("v2")
    assert set(iter_store_keys(store.store)) == source_keys
    xr.testing.assert_identical(store.get().compute(), dataset)


def test_failed_new_version_copy_cleans_up(store, dataset, monkeypatch, tmp_path):
    store.create_version("v1", checkout=True)
    dataset.to_zarr(store.store, mode="w", consolidated=True, zarr_format=2)
    source_keys = set(iter_store_keys(store.store))

    def fail_after_write(source, dest, **kwargs):
        copy_zarr_store(source, dest, source_path="x")
        assert list(iter_store_keys(dest))
        raise RuntimeError("copy failed")

    with monkeypatch.context() as patch:
        patch.setattr("openghg.storage._zarr_store.copy_zarr_store", fail_after_write)
        with pytest.raises(RuntimeError, match="copy failed"):
            store.create_version("v2", checkout=True, copy_current=True)

    assert store.versions == ["v1"]
    assert store.current_version == "v1"
    assert set(iter_store_keys(store.store)) == source_keys
    xr.testing.assert_identical(store.get().compute(), dataset)
    if (tmp_path / "versions").exists():
        reopened = get_versioned_zarr_directory_store(tmp_path / "versions")
        assert reopened.versions == ["v1"]

    # The same name is reusable after cleanup and the successful copy is isolated.
    store.create_version("v2", checkout=True, copy_current=True)
    dataset.assign(x=dataset.x + 10).to_zarr(store.store, mode="w", consolidated=True, zarr_format=2)
    store.checkout_version("v1")
    xr.testing.assert_identical(store.get().compute(), dataset)


def test_copy_current_version_is_noop(store, dataset, monkeypatch):
    store.create_version("v1", checkout=True)
    dataset.to_zarr(store.store, mode="w", consolidated=True, zarr_format=2)

    def unexpected_copy(*args, **kwargs):
        pytest.fail("Copying to the current version must not clear or copy its data.")

    monkeypatch.setattr("openghg.storage._zarr_store.copy_zarr_store", unexpected_copy)
    store.copy_to_version("v1")
    xr.testing.assert_identical(store.get().compute(), dataset)


def test_copy_requires_selected_version(store):
    with pytest.raises(VersionError):
        store.copy_to_version("v1")
    assert store.versions == []
