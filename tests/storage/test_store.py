import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.storage import MemoryStore
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


@pytest.fixture()
def ms():
    """Test MemoryStore."""
    return MemoryStore()


def test_insert_creates(ms):
    assert not ms

    ms.insert(ds1)

    assert ms

    xr.testing.assert_equal(ms.get(), ds1)


def test_clear(ms):
    assert not ms

    ms.insert(ds1)

    assert ms

    ms.clear()

    assert not ms
    xr.testing.assert_identical(ms.get(), xr.Dataset())


def test_insert_twice(ms):
    ms.insert(ds1)
    ms.insert(ds5)

    expected = np.hstack([ds1.x.values, ds5.x.values])

    np.testing.assert_equal(ms.get().x.values, expected)


def test_error_on_insert_conflict(ms):
    ms.insert(ds1)

    with pytest.raises(DataOverlapError):
        ms.insert(ds1)


def test_insert_ignore_conflict(ms):
    ms.insert(ds1)

    # all values should be ignored
    ms.insert(ds1, on_conflict="ignore")

    # ds4 has four non-conflicting values, so four values should be added
    ms.insert(ds4, on_conflict="ignore")

    # check values
    expected = np.hstack([ds1.x.values, ds4.x.values[-4:]])

    np.testing.assert_equal(ms.get().x.values, expected)


def test_update(ms):
    ms.insert(ds1)

    xr.testing.assert_equal(ms.get(), ds1)

    # update the values
    twice_ds1 = ds1.map(lambda x: 2 * x)
    ms.update(twice_ds1)

    xr.testing.assert_equal(ms.get(), twice_ds1)


def test_update_ignore_nonconflicts(ms):
    ms.insert(ds1)

    xr.testing.assert_equal(ms.get(), ds1)

    # update the values with "ignore"
    ms.update(ds4, on_nonconflict="ignore")

    expected = np.hstack([ds1.x.values[:4], ds4.x.values[:-4]])
    np.testing.assert_equal(ms.get().x.values, expected)
