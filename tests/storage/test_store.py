import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.storage import MemoryStore
from openghg.storage._zarr_store import get_zarr_directory_store, get_zarr_memory_store
from openghg.types import DataOverlapError


def dummy_dataset(index, data=None) -> xr.Dataset:
    if data is None:
        data = np.arange(len(index))
    ds = xr.Dataset({"x": ("time", data)}, coords={"time": index})
    return ds


idx1 = pd.date_range("2020-01-01", "2020-01-02", freq="h", inclusive="left")
idx2 = idx1[:5].union(idx1[10:])  # missing indices 5, 6, 7, 8, 9
idx3 = idx1[:5].union(idx1[5:10] + pd.Timedelta("1m")).union(idx1[10:])  # modified indices 5, 6, 7, 8, 9
idx4 = idx1 + pd.Timedelta("4h")  # shift by 4 hours
idx5 = pd.date_range("2020-01-02", "2020-01-03", freq="h", inclusive="left")  # index starting after idx1

ds1 = dummy_dataset(idx1)
ds2 = dummy_dataset(idx2)
ds3 = dummy_dataset(idx3)
ds4 = dummy_dataset(idx4)
ds5 = dummy_dataset(idx5)

# test data for updating
twice_ds1 = dummy_dataset(idx1).map(lambda x: 2 * x)
twice_ds2 = dummy_dataset(idx2).map(lambda x: 2 * x)  # non-contiguous with idx1


@pytest.fixture()
def memory_store():
    """Test MemoryStore."""
    return MemoryStore()


@pytest.fixture()
def zarr_memory_store():
    """Test ZarrStore with zarr.MemoryStore as underlying storage."""
    return get_zarr_memory_store()


@pytest.fixture()
def zarr_directory_store(tmp_path):
    """Test ZarrStore with zarr.DirectoryStore as underlying storage."""
    return get_zarr_directory_store(path=tmp_path)


# names of fixtures to use in parametrize
store_names = ["memory_store", "zarr_memory_store", "zarr_directory_store"]


# To use fixtures in parametrize, use the "request" fixture, as detailed here:
# https://stackoverflow.com/questions/42014484/pytest-using-fixtures-as-arguments-in-parametrize
@pytest.mark.parametrize("store_name", store_names)
def test_insert_creates(store_name, request):
    store = request.getfixturevalue(store_name)

    assert not store

    store.insert(ds1)

    assert store

    xr.testing.assert_equal(store.get(), ds1)


@pytest.mark.parametrize("store_name", store_names)
def test_clear(store_name, request):
    store = request.getfixturevalue(store_name)

    assert not store

    store.insert(ds1)

    assert store

    store.clear()

    assert not store
    xr.testing.assert_identical(store.get(), xr.Dataset())


@pytest.mark.parametrize("store_name", store_names)
def test_insert_twice(store_name, request):
    store = request.getfixturevalue(store_name)

    store.insert(ds1)
    store.insert(ds5)

    expected = np.hstack([ds1.x.values, ds5.x.values])

    np.testing.assert_equal(store.get().x.values, expected)


@pytest.mark.parametrize("store_name", store_names)
def test_error_on_insert_conflict(store_name, request):
    """Test an error is raised on conflict."""
    store = request.getfixturevalue(store_name)

    store.insert(ds1)

    with pytest.raises(DataOverlapError):
        store.insert(ds1)


@pytest.mark.parametrize("store_name", store_names)
def test_insert_ignore_conflict(store_name, request):
    """Test that insert with `on_conflict = 'ignore'` inserts non-conflicting values."""
    store = request.getfixturevalue(store_name)

    store.insert(ds1)

    # all values should be ignored
    store.insert(ds1, on_conflict="ignore")

    # ds4 has four non-conflicting values, so four values should be added
    store.insert(ds4, on_conflict="ignore")

    # check values
    expected = np.hstack([ds1.x.values, ds4.x.values[-4:]])

    np.testing.assert_equal(store.get().x.values, expected)


@pytest.mark.parametrize("store_name", store_names)
def test_update(store_name, request):
    store = request.getfixturevalue(store_name)

    store.insert(ds1)

    xr.testing.assert_equal(store.get(), ds1)

    # update the values
    store.update(twice_ds1)

    xr.testing.assert_equal(store.get(), twice_ds1)


@pytest.mark.parametrize("store_name", store_names)
def test_update_ignore_nonconflicts(store_name, request):
    """Test `update` with non-conflicts ignored."""
    store = request.getfixturevalue(store_name)

    store.insert(ds1)

    xr.testing.assert_equal(store.get(), ds1)

    # update the values with "ignore"
    store.update(ds4, on_nonconflict="ignore")

    expected = np.hstack([ds1.x.values[:4], ds4.x.values[:-4]])
    np.testing.assert_equal(store.get().x.values, expected)


@pytest.mark.parametrize("store_name, is_zarr", [(name, "zarr" in name) for name in store_names])
def test_non_contiguous_update(store_name, is_zarr, request):
    """Test `update` when only some of the values are updated.

    This raises an error with Zarr stores because the region to update is non-contiguous.
    We will fix this later.
    """
    store = request.getfixturevalue(store_name)

    store.insert(ds1)

    xr.testing.assert_equal(store.get(), ds1)

    # update the values in a non-contiguous region
    if not is_zarr:
        store.update(twice_ds2)

        expected = np.hstack([twice_ds2.x.values[:5], ds1.x.values[5:10], twice_ds2.x.values[5:]])

        np.testing.assert_equal(store.get().x.values, expected)
    else:
        with pytest.raises(NotImplementedError):
            store.update(twice_ds2)


@pytest.mark.parametrize("store_name", store_names)
def test_contiguous_update(store_name, request):
    """Test `update` on contiguous region of times."""
    store = request.getfixturevalue(store_name)

    store.insert(ds1)

    # insert data starting after data in ds1
    store.insert(ds5)

    # update contiguous region (same region as ds1)
    store.update(twice_ds1)

    expected = np.hstack([twice_ds1.x.values, ds5.x.values])
    np.testing.assert_equal(store.get().x.values, expected)
