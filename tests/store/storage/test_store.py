"""
Tests for implementations of Store.
"""

import pytest

import numpy as np
import pandas as pd
import xarray as xr

from openghg.store.storage import MemoryStore
from openghg.types import DataOverlapError, UpdateError


def dummy_dataset(index, data=None) -> xr.Dataset:
    if data is None:
        data = np.arange(len(index))
    ds = xr.Dataset({"x": ("time", data)}, coords={"time": index})
    return ds

@pytest.fixture
def idx1():
    """Base index with values on the hour for one day."""
    return pd.date_range("2020-01-01", "2020-01-02", freq="h", inclusive="left")


@pytest.fixture
def idx2(idx1):
    """Base index with missing values from indicies 5, 6, 7, 8, 9"""
    return idx1[:5].union(idx1[10:])


@pytest.fixture
def idx3(idx1):
    """Base index with modified values at indicies 5, 6, 7, 8, 9"""
    return idx1[:5].union(idx1[5:10] + pd.Timedelta("1m")).union(idx1[10:])


@pytest.fixture
def idx4(idx1):
    """Base index shifted forward by 4 hours"""
    return idx1 + pd.Timedelta("4h")

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
def ds5():
    idx = pd.date_range("2020-01-02", "2020-01-03", freq="h", inclusive="left")
    return dummy_dataset(idx)


class TestMemoryStore():
    def test_insert_on_empty_creates(self, ds1):
        ms = MemoryStore()

        assert len(ms) == 0
        assert ms.data is None

        ms.insert(ds1)

        assert len(ms) == len(ds1.time)

    def test_insert_and_clear(self, ds1):
        ms = MemoryStore()

        ms.insert(ds1)

        assert len(ms) == len(ds1.time)

        ms.clear()

        assert len(ms) == 0
        assert ms.data is None

    def test_insert_twice(self, ds1, ds5):
        ms = MemoryStore()
        ms.insert(ds1)
        ms.insert(ds5)

        assert len(ms) == len(ds1.time) + len(ds5.time)

        expected = np.hstack([ds1.x.values, ds5.x.values])

        np.testing.assert_equal(ms.get().x.values, expected)

    def test_insert_raises_on_conflict(self, ds1):
        ms = MemoryStore()
        ms.insert(ds1)

        with pytest.raises(DataOverlapError):
            ms.insert(ds1)

    def test_insert_ignore_flag(self, ds1, ds4):
        ms = MemoryStore()
        ms.insert(ds1)

        len1 = len(ms)

        # all values should be ignored
        ms.insert(ds1, on_conflict="ignore")

        assert len(ms) == len1

        # ds4 has four non-conflicting values, so four values should be added
        ms.insert(ds4, on_conflict="ignore")

        assert len(ms) == len1 + 4

        # check values
        expected = np.hstack([ds1.x.values, ds4.x.values[-4:]])

        np.testing.assert_equal(ms.get().x.values, expected)

    def test_update(self, ds1):
        ms = MemoryStore()
        ms.insert(ds1)

        xr.testing.assert_equal(ms.get(), ds1)

        # update the values
        twice_ds1 = ds1.map(lambda x: 2 * x)
        ms.update(twice_ds1)

        xr.testing.assert_equal(ms.get(), twice_ds1)

    def test_update_empty_store_error(self, ds1):
        ms = MemoryStore()

        with pytest.raises(UpdateError):
            ms.update(ds1)

    def test_update_raises_on_nonconflict(self, ds1, ds4):
        ms = MemoryStore()
        ms.insert(ds1)

        with pytest.raises(UpdateError):
            ms.update(ds4)

    def test_update_ignore_flag(self, ds1, ds4):
        """Check that update with `on_nonconflict == "ignore"` only updates
        overlapping times.
        """
        ms = MemoryStore()
        ms.insert(ds1)

        xr.testing.assert_equal(ms.get(), ds1)

        # update the values with "ignore"
        ms.update(ds4, on_nonconflict="ignore")

        expected = np.hstack([ds1.x.values[:4], ds4.x.values[:-4]])
        np.testing.assert_equal(ms.get().x.values, expected)

    def test_upsert(self, ds1, ds4):
        """Check that upsert writes all new values."""
        ms = MemoryStore()
        ms.insert(ds1)
        ms.upsert(ds4)

        expected = np.hstack([ds1.x.values[:4], ds4.x.values])
        np.testing.assert_equal(ms.get().x.values, expected)

    def test_overwrite(self, ds1, ds2):
        ms = MemoryStore()
        ms.insert(ds1)

        xr.testing.assert_equal(ms.get(), ds1)

        ms.overwrite(ds2)

        xr.testing.assert_equal(ms.get(), ds2)

    def test_copy(self, ds1, ds2):
        ms1 = MemoryStore()
        ms1.insert(ds1)

        ms2 = MemoryStore()
        ms1.copy(ms2)

        xr.testing.assert_equal(ms1.get(), ms2.get())

        # copy overwrites: check by overwriting ms2, then
        # copying to it again
        ms2.overwrite(ds2)

        xr.testing.assert_equal(ms2.get(), ds2)

        ms1.copy(ms2)

        xr.testing.assert_equal(ms1.get(), ms2.get())
