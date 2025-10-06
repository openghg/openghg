from numcodecs import Blosc
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.storage import (
    MemoryStore,
    VersionedMemoryStore,
    get_zarr_directory_store,
    get_zarr_memory_store,
    get_versioned_zarr_directory_store,
    get_versioned_zarr_memory_store,
)
from openghg.storage._store import VersionedStore
from openghg.storage._zarr_store import ZarrStore, VersionedZarrStore
from openghg.types import DataOverlapError
from openghg.util._versioning import SimpleVersioning, VersionError


# idx1 = pd.date_range("2020-01-01", "2020-01-02", freq="h", inclusive="left")
# idx2 = idx1[:5].union(idx1[10:])  # missing indices 5, 6, 7, 8, 9
# idx3 = idx1[:5].union(idx1[5:10] + pd.Timedelta("1m")).union(idx1[10:])  # modified indices 5, 6, 7, 8, 9
# idx4 = idx1 + pd.Timedelta("4h")  # shift by 4 hours
# idx5 = pd.date_range("2020-01-02", "2020-01-03", freq="h", inclusive="left")  # index starting after idx1

# ds1 = dummy_dataset(idx1)
# ds2 = dummy_dataset(idx2)
# ds3 = dummy_dataset(idx3)
# ds4 = dummy_dataset(idx4)
# ds5 = dummy_dataset(idx5)

# # test data for updating
# twice_ds1 = dummy_dataset(idx1).map(lambda x: 2 * x)
# twice_ds2 = dummy_dataset(idx2).map(lambda x: 2 * x)  # non-contiguous with idx1

# DATA FIXTURES
def dummy_dataset(index, data=None) -> xr.Dataset:
    if data is None:
        data = np.arange(len(index))
    ds = xr.Dataset({"x": ("time", data)}, coords={"time": index})
    return ds


@pytest.fixture
def idx1():
    return pd.date_range("2020-01-01", "2020-01-02", freq="h", inclusive="left")


@pytest.fixture
def idx2(idx1):
    return idx1[:5].union(idx1[10:])  # missing indices 5, 6, 7, 8, 9


@pytest.fixture
def idx3(idx1):
    return idx1[:5].union(idx1[5:10] + pd.Timedelta("1m")).union(idx1[10:])  # modified indices 5, 6, 7, 8, 9


@pytest.fixture
def idx4(idx1):
    return idx1 + pd.Timedelta("4h")  # shift by 4 hours


@pytest.fixture
def idx5():
    return pd.date_range("2020-01-02", "2020-01-03", freq="h", inclusive="left")  # index starting after idx1


@pytest.fixture
def ds1(idx1):
    return dummy_dataset(idx1)


@pytest.fixture
def ds2(idx2):
    return dummy_dataset(idx2)


@pytest.fixture
def ds3(idx3):
    return dummy_dataset(idx3)


@pytest.fixture
def ds4(idx4):
    return dummy_dataset(idx4)


@pytest.fixture
def ds5(idx5):
    return dummy_dataset(idx5)


# test data for updating
@pytest.fixture
def twice_ds1(idx1):
    return dummy_dataset(idx1).map(lambda x: 2 * x)


@pytest.fixture
def twice_ds2(idx2):
    return dummy_dataset(idx2).map(lambda x: 2 * x)  # non-contiguous with idx1


# STORE FIXTURES
@pytest.fixture()
def memory_store():
    """Test MemoryStore."""
    return MemoryStore()


@pytest.fixture()
def versioned_memory_store():
    """Test VersionedMemoryStore."""
    return VersionedMemoryStore()


@pytest.fixture()
def zarr_memory_store():
    """Test ZarrStore with zarr.MemoryStore as underlying storage."""
    return get_zarr_memory_store()


@pytest.fixture()
def zarr_directory_store(tmp_path):
    """Test ZarrStore with zarr.DirectoryStore as underlying storage."""
    return get_zarr_directory_store(path=tmp_path)


@pytest.fixture()
def versioned_zarr_memory_store():
    """Test VersionedZarrStore with zarr.MemoryStore as underlying storage."""
    return get_versioned_zarr_memory_store()


@pytest.fixture()
def versioned_zarr_directory_store(tmp_path):
    """Test VersionedZarrStore with zarr.DirectoryStore as underlying storage."""
    return get_versioned_zarr_directory_store(path=tmp_path)


# names of fixtures to use in parametrize
store_names = [
    "memory_store",
    "versioned_memory_store",
    "zarr_memory_store",
    "zarr_directory_store",
    "versioned_zarr_memory_store",
    "versioned_zarr_directory_store",
]
store_names_is_versioned = [(name, "versioned" in name) for name in store_names]


# TESTS


# To use fixtures in parametrize, use the "request" fixture, as detailed here:
# https://stackoverflow.com/questions/42014484/pytest-using-fixtures-as-arguments-in-parametrize
@pytest.mark.parametrize("store_name, is_versioned", store_names_is_versioned)
def test_insert_creates(store_name, is_versioned, request, ds1):
    store = request.getfixturevalue(store_name)

    if is_versioned:
        store.create_version("v1", checkout=True)

    assert not store

    store.insert(ds1)

    assert store

    xr.testing.assert_equal(store.get(), ds1)


@pytest.mark.parametrize("store_name, is_versioned", store_names_is_versioned)
def test_clear(store_name, is_versioned, request, ds1):
    store = request.getfixturevalue(store_name)

    if is_versioned:
        store.create_version("v1", checkout=True)

    assert not store

    store.insert(ds1)

    assert store

    store.clear()

    assert not store
    xr.testing.assert_identical(store.get(), xr.Dataset())


@pytest.mark.parametrize("store_name, is_versioned", store_names_is_versioned)
def test_insert_twice(store_name, is_versioned, request, ds1, ds5):
    store = request.getfixturevalue(store_name)

    if is_versioned:
        store.create_version("v1", checkout=True)

    store.insert(ds1)
    store.insert(ds5)

    expected = np.hstack([ds1.x.values, ds5.x.values])

    np.testing.assert_equal(store.get().x.values, expected)


@pytest.mark.parametrize("store_name, is_versioned", store_names_is_versioned)
def test_error_on_insert_overlap(store_name, is_versioned, request, ds1):
    """Test an error is raised on overlap."""
    store = request.getfixturevalue(store_name)

    if is_versioned:
        store.create_version("v1", checkout=True)

    store.insert(ds1)

    with pytest.raises(DataOverlapError):
        store.insert(ds1)


@pytest.mark.parametrize("store_name, is_versioned", store_names_is_versioned)
def test_insert_ignore_overlap(store_name, is_versioned, request, ds1, ds4):
    """Test that insert with `on_overlap = 'ignore'` inserts non-overlaping values."""
    store = request.getfixturevalue(store_name)

    if is_versioned:
        store.create_version("v1", checkout=True)

    store.insert(ds1)

    # all values should be ignored
    store.insert(ds1, on_overlap="ignore")

    # ds4 has four non-overlaping values, so four values should be added
    store.insert(ds4, on_overlap="ignore")

    # check values
    expected = np.hstack([ds1.x.values, ds4.x.values[-4:]])

    np.testing.assert_equal(store.get().x.values, expected)


@pytest.mark.parametrize("store_name, is_versioned", store_names_is_versioned)
def test_update(store_name, is_versioned, request, ds1, twice_ds1):
    store = request.getfixturevalue(store_name)

    if is_versioned:
        store.create_version("v1", checkout=True)

    store.insert(ds1)

    xr.testing.assert_equal(store.get(), ds1)

    # update the values
    store.update(twice_ds1)

    xr.testing.assert_equal(store.get(), twice_ds1)


@pytest.mark.parametrize("store_name, is_versioned", store_names_is_versioned)
def test_update_ignore_nonoverlaps(store_name, is_versioned, request, ds1, ds4):
    """Test `update` with non-overlaps ignored."""
    store = request.getfixturevalue(store_name)

    if is_versioned:
        store.create_version("v1", checkout=True)

    store.insert(ds1)

    xr.testing.assert_equal(store.get(), ds1)

    # update the values with "ignore"
    store.update(ds4, on_nonoverlap="ignore")

    expected = np.hstack([ds1.x.values[:4], ds4.x.values[:-4]])
    np.testing.assert_equal(store.get().x.values, expected)


@pytest.mark.parametrize(
    "store_name, is_versioned, is_zarr", [(name, "versioned" in name, "zarr" in name) for name in store_names]
)
def test_non_contiguous_update(store_name, is_versioned, is_zarr, request, ds1, twice_ds2):
    """Test `update` when only some of the values are updated.

    This raises an error with Zarr stores because the region to update is non-contiguous.
    We will fix this later.
    """
    store = request.getfixturevalue(store_name)

    if is_versioned:
        store.create_version("v1", checkout=True)

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


@pytest.mark.parametrize("store_name, is_versioned", store_names_is_versioned)
def test_contiguous_update(store_name, is_versioned, request, ds1, twice_ds1, ds5):
    """Test `update` on contiguous region of times."""
    store = request.getfixturevalue(store_name)

    if is_versioned:
        store.create_version("v1", checkout=True)

    store.insert(ds1)

    # insert data starting after data in ds1
    store.insert(ds5)

    # update contiguous region (same region as ds1)
    store.update(twice_ds1)

    expected = np.hstack([twice_ds1.x.values, ds5.x.values])
    np.testing.assert_equal(store.get().x.values, expected)


# ZARR SPECIFIC TESTS
@pytest.mark.parametrize("store_name", [name for name in store_names if "zarr" in name])
def test_zarr_encoding(store_name, request, ds1):
    """Check that data can be compressed with a specified encoding."""
    store: ZarrStore = request.getfixturevalue(store_name)

    compressor = Blosc(cname="zstd", clevel=5, shuffle=1)
    encoding = {dv: {"compressor": compressor} for dv in ds1.data_vars}

    # set `to_zarr_kwargs` here since it wasn't passed to init
    store.to_zarr_kwargs = {"encoding": encoding}

    if isinstance(store, VersionedZarrStore):
        store.create_version("v1", checkout=True)

    store.insert(ds1)

    ds = store.get()

    for dv in ds.data_vars:
        assert ds[dv].encoding["compressor"] == compressor


# TESTS FOR VERSIONED STORES
versioned_store_names = [name for name in store_names if "versioned" in name]


@pytest.mark.parametrize("store_name", versioned_store_names)
def test_add_data_to_new_version(store_name, request, ds1, ds5):
    """Check that data can be added to a new version without affecting an old version."""
    store = request.getfixturevalue(store_name)

    # create a version and add some data
    store.create_version("v1", checkout=True)
    store.insert(ds1)

    # create a new version, copying v1, then add some more data
    store.create_version("v2", checkout=True, copy_current=True)
    store.insert(ds5)

    # version 2 is still checked out, and should have the combined values added
    expected2 = np.hstack([ds1.x.values, ds5.x.values])
    np.testing.assert_equal(store.get().x.values, expected2)

    # checkout version 1 and test that it is unmodified
    store.checkout_version("v1")
    expected1 = ds1.x.values
    np.testing.assert_equal(store.get().x.values, expected1)


@pytest.mark.parametrize("store_name", versioned_store_names)
def test_nbytes_stored(store_name, request, ds1):
    """Check that for subclasses of VersionedStore, `bytes_stored` sums over versions."""
    store = request.getfixturevalue(store_name)

    # create a version and add some data
    store.create_version("v1", checkout=True)
    store.insert(ds1)

    nbytes1 = store.bytes_stored()

    # create a version and add same data
    store.create_version("v2", checkout=True)
    store.insert(ds1)

    nbytes2 = store.bytes_stored()

    assert nbytes2 == 2 * nbytes1


@pytest.mark.parametrize("store_name", versioned_store_names)
def test_bool(store_name, request, ds1):
    """Check that for subclasses of VersionedStore, ``bool`` looks to see if any version is non-empty."""
    store = request.getfixturevalue(store_name)

    # create a version and add some data
    store.create_version("v1", checkout=True)
    store.insert(ds1)

    # create another (empty) version
    store.create_version("v2", checkout=True)

    # check that current version evaluates to False
    # this uses internals of SimpleVersioning
    if isinstance(store, SimpleVersioning):
        assert not store._versions["v2"]

    # check that versioned store evaluates to True
    assert store


@pytest.mark.parametrize("store_name", versioned_store_names)
def test_clear_data_from_old_version(store_name, request, ds1):
    """Check that data can be cleared from an old version without affecting a new version."""
    store = request.getfixturevalue(store_name)

    # create a version and add some data
    store.create_version("v1", checkout=True)
    store.insert(ds1)

    # create a new version, copying version 1
    store.create_version("v2", checkout=True, copy_current=True)

    # checkout v1 and clear it
    store.checkout_version("v1")
    store.clear()
    assert not store

    # check out v2 and test that it is not empty
    store.checkout_version("v2")
    assert store


@pytest.mark.parametrize("store_name", versioned_store_names)
def test_delete_data_from_old_version(store_name, request, ds1):
    """Check that an old version can be deleted without affecting a new version."""
    store = request.getfixturevalue(store_name)

    # create a version and add some data
    store.create_version("v1", checkout=True)
    store.insert(ds1)

    # create a new version, copying version 1
    store.create_version("v2", checkout=True, copy_current=True)

    # checkout v1 and clear it
    store.checkout_version("v1")
    store.delete_version("v1")

    # now there is no version checked out
    with pytest.raises(VersionError):
        store.current_version

    # check out v2 and test that it is not empty
    store.checkout_version("v2")
    assert store
