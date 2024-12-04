"""
Tests for implementations of StoreIndex.
"""

import pytest

import numpy as np
import pandas as pd
import xarray as xr

from openghg.store.storage import DatetimeStoreIndex, FloorDatetimeStoreIndex


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


class TestDatetimeStoreIndex:
    def test_len(self, idx1):
        dtsi = DatetimeStoreIndex(idx1)

        assert len(dtsi) == 24

    def test_from_dataset(self, idx1):
        dtsi = DatetimeStoreIndex(idx1)
        ds = dummy_dataset(idx1)
        dtsi_from_ds = DatetimeStoreIndex.from_dataset(ds)

        assert dtsi == dtsi_from_ds

    @pytest.mark.parametrize("index_a", [(1, "idx1"), (2, "idx2"), (3, "idx3"), (4, "idx4")])
    @pytest.mark.parametrize("index_b", [(1, "idx1"), (2, "idx2"), (3, "idx3"), (4, "idx4")])
    def test_conflicts_found(self, index_a, index_b, request):
        _, idx_a0 = index_a
        _, idx_b0 = index_b

        idx_a = request.getfixturevalue(idx_a0)
        dtsi_a = DatetimeStoreIndex(idx_a)

        idx_b = request.getfixturevalue(idx_b0)
        dtsi_b = DatetimeStoreIndex(idx_b)
        ds = dummy_dataset(idx_b)

        conflicts = dtsi_a.conflicts_found(dtsi_b)
        ds_conflicts = dtsi_a.conflicts_found(ds)

        assert conflicts
        assert ds_conflicts

    @pytest.mark.parametrize("index_a", [(1, "idx1"), (2, "idx2"), (3, "idx3"), (4, "idx4")])
    @pytest.mark.parametrize("index_b", [(1, "idx1"), (2, "idx2"), (3, "idx3"), (4, "idx4")])
    def test_nonconflicts_found(self, index_a, index_b, request):
        a, idx_a0 = index_a
        b, idx_b0 = index_b

        idx_a = request.getfixturevalue(idx_a0)
        dtsi_a = DatetimeStoreIndex(idx_a)

        idx_b = request.getfixturevalue(idx_b0)
        dtsi_b = DatetimeStoreIndex(idx_b)
        ds = dummy_dataset(idx_b)

        nonconflicts = dtsi_a.nonconflicts_found(dtsi_b)
        ds_nonconflicts = dtsi_a.nonconflicts_found(ds)

        if b == 2 and a in (1, 3):
            # all values in idx2 are values in idx1 and idx3
            # so idx2 has no non-conflicts with idx1 and idx3
            assert not nonconflicts
            assert not ds_nonconflicts
        elif a != b:
            assert nonconflicts
            assert ds_nonconflicts
        else:
            assert not nonconflicts
            assert not ds_nonconflicts

    @pytest.mark.parametrize("index_a", [(1, "idx1"), (2, "idx2"), (3, "idx3"), (4, "idx4")])
    @pytest.mark.parametrize("index_b", [(1, "idx1"), (2, "idx2"), (3, "idx3"), (4, "idx4")])
    def test_conflicts(self, index_a, index_b, request, idx1, idx2):
        a, idx_a0 = index_a
        b, idx_b0 = index_b

        idx_a = request.getfixturevalue(idx_a0)
        dtsi_a = DatetimeStoreIndex(idx_a)

        idx_b = request.getfixturevalue(idx_b0)
        dtsi_b = DatetimeStoreIndex(idx_b)
        ds = dummy_dataset(idx_b)

        conflicts = dtsi_a.conflicts(dtsi_b)
        ds_conflicts = dtsi_a.conflicts(ds)

        pair = [a, b]

        match pair:
            case [x, y] if x == y:
                # a == b --> all of index a and index b are conflicts
                assert conflicts == dtsi_a
                assert conflicts == dtsi_b
                assert ds_conflicts == dtsi_a
                assert ds_conflicts == dtsi_b
            case [1, (2 | 3)] | [(2 | 3), 1] | [2, 3] | [3, 2]:
                # unordered pairs: {1, 2}, {1, 3}, {2, 3}
                # indexes 1, 2, 3 have pairwise distinct values at indices 5, 6, 7, 8, 9
                # so conflicts are values outside those indices, which is the definition of idx2
                expected =  DatetimeStoreIndex(idx2)
                assert conflicts == expected
                assert ds_conflicts == expected
            case [1, 4] | [4, 1]:
                # unordered pairs {1, 4}
                # no conflicts on first 4 values
                expected =  DatetimeStoreIndex(idx1[4:])
                assert conflicts == expected
                assert ds_conflicts == expected
            case [(2 | 3), 4] | [4, (2 | 3)]:
                # unordered pairs {2, 4}, {3, 4}
                # # no conflicts on first 4 values
                expected =  DatetimeStoreIndex(idx2[4:])
                assert conflicts == expected
                assert ds_conflicts == expected
            case _:
                # error if missing cases
                print(pair)
                assert False


    @pytest.mark.parametrize("index_a", [(1, "idx1"), (2, "idx2"), (3, "idx3"), (4, "idx4")])
    @pytest.mark.parametrize("index_b", [(1, "idx1"), (2, "idx2"), (3, "idx3"), (4, "idx4")])
    def test_nonconflicts(self, index_a, index_b, request):
        a, idx_a0 = index_a
        b, idx_b0 = index_b

        idx_a = request.getfixturevalue(idx_a0)
        dtsi_a = DatetimeStoreIndex(idx_a)

        idx_b = request.getfixturevalue(idx_b0)
        dtsi_b = DatetimeStoreIndex(idx_b)
        ds = dummy_dataset(idx_b)

        nonconflicts = dtsi_a.nonconflicts(dtsi_b)
        ds_nonconflicts = dtsi_a.nonconflicts(ds)

        pair = [a, b]

        match pair:
            case [x, y] if x == y:
                # a == b --> no non-conflicts
                assert nonconflicts == DatetimeStoreIndex()
                assert ds_nonconflicts == DatetimeStoreIndex()
            case [(1 | 3), 2]:
                # idx2 is a subset of both idx1 and idx3, so no non-conflicts
                assert nonconflicts == DatetimeStoreIndex()
                assert ds_nonconflicts == DatetimeStoreIndex()
            case [2, (1 | 3)] | [1, 3] | [3, 1]:
                assert nonconflicts == DatetimeStoreIndex(idx_b[5:10])
                assert ds_nonconflicts == DatetimeStoreIndex(idx_b[5:10])
            case [4, (1 | 2)]:
                # no conflicts on first 4 values
                expected =  DatetimeStoreIndex(idx_b[:4])
                assert nonconflicts == expected
                assert ds_nonconflicts == expected
            case [4, 3]:
                # no conflicts on first 4 values and indicies 5, 6, 7, 8, 9
                expected = DatetimeStoreIndex(idx_b[:4].union(idx_b[5:10]))
                assert nonconflicts == expected
                assert ds_nonconflicts == expected
            case [(2 | 3), 4]:
                # no conflicts on last 4 values and indicies 1 to 6 (5 to 10 minus 4 due to 4h time shift)
                expected = DatetimeStoreIndex(idx_b[1:6].union(idx_b[-4:]))
                assert nonconflicts == expected
                assert ds_nonconflicts == expected
            case [1, 4]:
                # no conflicts on last 4 values
                expected =  DatetimeStoreIndex(idx_b[-4:])
                assert nonconflicts == expected
                assert ds_nonconflicts == expected
            case _:
                # error if missing cases
                print(pair)
                assert False


class TestFloorDatetimeStoreIndex:
    def test_len(self, idx1):
        fdtsi = FloorDatetimeStoreIndex(idx1)

        assert len(fdtsi) == 24

    @pytest.mark.parametrize("freq_a", ["1s", "1h"])
    @pytest.mark.parametrize("freq_b", ["1s", "1h"])
    def test_eq(self, idx1, freq_a, freq_b):
        fdtsi_a = FloorDatetimeStoreIndex(idx1, freq=freq_a)
        fdtsi_b = FloorDatetimeStoreIndex(idx1, freq=freq_b)

        if freq_a == freq_b:
            assert fdtsi_a == fdtsi_b
        else:
            assert fdtsi_a != fdtsi_b

    def test_from_dataset(self, idx1):
        fdtsi = FloorDatetimeStoreIndex(idx1)
        ds = dummy_dataset(idx1)
        fdtsi_from_ds = FloorDatetimeStoreIndex.from_dataset(ds)

        assert fdtsi == fdtsi_from_ds

    @pytest.mark.parametrize("index_a", [(1, "idx1"), (2, "idx2"), (3, "idx3"), (4, "idx4")])
    @pytest.mark.parametrize("index_b", [(1, "idx1"), (2, "idx2"), (3, "idx3"), (4, "idx4")])
    def test_conflicts_found(self, index_a, index_b, request):
        _, idx_a0 = index_a
        _, idx_b0 = index_b

        idx_a = request.getfixturevalue(idx_a0)
        fdtsi_a = FloorDatetimeStoreIndex(idx_a)

        idx_b = request.getfixturevalue(idx_b0)
        fdtsi_b = FloorDatetimeStoreIndex(idx_b)
        ds = dummy_dataset(idx_b)

        conflicts = fdtsi_a.conflicts_found(fdtsi_b)
        ds_conflicts = fdtsi_a.conflicts_found(ds)

        assert conflicts
        assert ds_conflicts

    @pytest.mark.parametrize("index_a", [(1, "idx1"), (2, "idx2"), (3, "idx3"), (4, "idx4")])
    @pytest.mark.parametrize("index_b", [(1, "idx1"), (2, "idx2"), (3, "idx3"), (4, "idx4")])
    def test_nonconflicts_found(self, index_a, index_b, request):
        a, idx_a0 = index_a
        b, idx_b0 = index_b

        idx_a = request.getfixturevalue(idx_a0)
        fdtsi_a = FloorDatetimeStoreIndex(idx_a)

        idx_b = request.getfixturevalue(idx_b0)
        fdtsi_b = FloorDatetimeStoreIndex(idx_b)
        ds = dummy_dataset(idx_b)

        nonconflicts = fdtsi_a.nonconflicts_found(fdtsi_b)
        ds_nonconflicts = fdtsi_a.nonconflicts_found(ds)

        if b == 2 and a in (1, 3):
            # all values in idx2 are values in idx1 and idx3
            # so idx2 has no non-conflicts with idx1 and idx3
            assert not nonconflicts
            assert not ds_nonconflicts
        elif a != b:
            assert nonconflicts
            assert ds_nonconflicts
        else:
            assert not nonconflicts
            assert not ds_nonconflicts

    def test_conflicts(self):
        pass

    def test_nonconflicts(self):
        pass
