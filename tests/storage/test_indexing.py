import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.storage._indexing import ConflictDeterminer


@pytest.fixture()
def cd_datetime_exact():
    idx = pd.date_range("2020-01-01", "2020-01-02", freq="1h")
    return ConflictDeterminer(idx)


@pytest.fixture()
def cd_datetime_nearest_1min():
    idx = pd.date_range("2020-01-01", "2020-01-02", freq="1h")
    return ConflictDeterminer(idx, method="nearest", tolerance=pd.Timedelta("60s"))


# times used in tests
times = ["2020-01-01", "2020-01-01 00:00:49", "2020-01-01 00:30:00", "2020-01-02", "2020-01-02 00:01:01"]


@pytest.mark.parametrize(
    ("other", "expected"),
    [
        (["2020-01-01"], [True]),
        (["2020-01-01 00:00:01"], [False]),
        (times, [True, False, False, True, False]),
    ],
)
def test_conflicts_exact(cd_datetime_exact, other, expected):
    assert cd_datetime_exact.has_conflicts(other) == any(expected)
    assert cd_datetime_exact.has_nonconflicts(other) == any([not exp for exp in expected])
    assert all(cd_datetime_exact.conflicts(other) == expected)


@pytest.mark.parametrize(
    ("other", "expected"),
    [
        (["2020-01-01"], [True]),
        (["2020-01-01 00:00:01"], [True]),
        (times, [True, True, False, True, False]),
    ],
)
def test_conflicts_nearest_1min(cd_datetime_nearest_1min, other, expected):
    assert cd_datetime_nearest_1min.has_conflicts(other) == any(expected)
    assert cd_datetime_nearest_1min.has_nonconflicts(other) == any([not exp for exp in expected])
    assert all(cd_datetime_nearest_1min.conflicts(other) == expected)


def test_select_conflicts_nonconflicts(cd_datetime_nearest_1min):
    """Test selecting conflicts and non-conflicts from Dataset."""
    ds = xr.Dataset({"x": (["time"], np.arange(len(times)))}, coords={"time": (["time"], times)})

    xr.testing.assert_equal(ds.isel(time=[0, 1, 3]), cd_datetime_nearest_1min.select_conflicts(ds, "time"))

    xr.testing.assert_equal(ds.isel(time=[2, 4]), cd_datetime_nearest_1min.select_nonconflicts(ds, "time"))
