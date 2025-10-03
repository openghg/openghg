import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.storage._indexing import OverlapDeterminer


@pytest.fixture()
def cd_datetime_exact():
    idx = pd.date_range("2020-01-01", "2020-01-02", freq="1h")
    return OverlapDeterminer(idx)


@pytest.fixture()
def cd_datetime_nearest_1min():
    idx = pd.date_range("2020-01-01", "2020-01-02", freq="1h")
    return OverlapDeterminer(idx, method="nearest", tolerance=pd.Timedelta("60s"))


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
def test_overlaps_exact(cd_datetime_exact, other, expected):
    assert cd_datetime_exact.has_overlaps(other) == any(expected)
    assert cd_datetime_exact.has_nonoverlaps(other) == any([not exp for exp in expected])
    assert all(cd_datetime_exact.overlaps(other) == expected)


@pytest.mark.parametrize(
    ("other", "expected"),
    [
        (["2020-01-01"], [True]),
        (["2020-01-01 00:00:01"], [True]),
        (times, [True, True, False, True, False]),
    ],
)
def test_overlaps_nearest_1min(cd_datetime_nearest_1min, other, expected):
    assert cd_datetime_nearest_1min.has_overlaps(other) == any(expected)
    assert cd_datetime_nearest_1min.has_nonoverlaps(other) == any([not exp for exp in expected])
    assert all(cd_datetime_nearest_1min.overlaps(other) == expected)


def test_select_overlaps_nonoverlaps(cd_datetime_nearest_1min):
    """Test selecting overlaps and non-overlaps from Dataset."""
    ds = xr.Dataset({"x": (["time"], np.arange(len(times)))}, coords={"time": (["time"], times)})

    xr.testing.assert_equal(ds.isel(time=[0, 1, 3]), cd_datetime_nearest_1min.select_overlaps(ds, "time"))

    xr.testing.assert_equal(ds.isel(time=[2, 4]), cd_datetime_nearest_1min.select_nonoverlaps(ds, "time"))
