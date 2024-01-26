import numpy as np
import pytest
import xarray as xr
from openghg.store import infer_date_range, update_zero_dim
from openghg.util import timestamp_tzaware
from pandas import Timedelta
from xarray import DataArray

# %% Infer period when one time point present


@pytest.fixture(scope="module")
def time_1_point():
    """Create data with one time point"""
    np_time = np.array(["2012-01-01"], dtype="datetime64[ns]")
    time = DataArray(np_time, dims=("time"), coords={"time": np_time})

    return time


def test_infer_period_file_year(time_1_point):
    """Test frequency can be inferred from filename - yearly"""
    filepath = "/path/to/directory/ch4_EUROPE_2012.nc"

    start_date, end_date, period_str = infer_date_range(time_1_point, filepath)

    assert start_date == timestamp_tzaware("2012-01-01 00:00:00")
    assert end_date == timestamp_tzaware("2013-01-01 00:00:00") - Timedelta(seconds=1)
    assert period_str == "1 year"


def test_infer_period_file_month(time_1_point):
    """Test frequency can be inferred from filename - monthly"""
    filepath = "/path/to/directory/ch4_EUROPE_201201.nc"

    start_date, end_date, period_str = infer_date_range(time_1_point, filepath)

    assert start_date == timestamp_tzaware("2012-01-01")
    assert end_date == timestamp_tzaware("2012-02-01") - Timedelta(seconds=1)
    assert period_str == "1 month"


def test_infer_period_from_input(time_1_point):
    """Test period can be set and can supercede value inferred from filename"""
    filepath = "/path/to/directory/ch4_EUROPE_201201.nc"
    period = "yearly"

    start_date, end_date, period_str = infer_date_range(time_1_point, filepath, period=period)

    assert start_date == timestamp_tzaware("2012-01-01")
    assert end_date == timestamp_tzaware("2013-01-01") - Timedelta(seconds=1)
    assert period_str == "1 year"


# %% Infer time period from data frequency


@pytest.fixture(scope="module")
def time_monthly():
    """Create monthly time data"""
    np_time = np.array(["2012-02-01", "2012-03-01", "2012-04-01"], dtype="datetime64[ns]")
    time = DataArray(np_time, dims=("time"), coords={"time": np_time})

    return time


def test_infer_period_from_frequency(time_monthly):
    """Test period can be inferred from time data"""
    start_date, end_date, period_str = infer_date_range(time_monthly)

    assert start_date == timestamp_tzaware("2012-02-01")
    assert end_date == timestamp_tzaware("2012-05-01") - Timedelta(seconds=1)
    assert period_str == "1 month"


def test_infer_period_from_frequency_ignore_input(time_monthly):
    """Test frequency is inferred from data even if period is set"""
    period = "1 year"
    start_date, end_date, period_str = infer_date_range(time_monthly, period=period)

    assert start_date == timestamp_tzaware("2012-02-01")
    assert end_date == timestamp_tzaware("2012-05-01") - Timedelta(seconds=1)
    assert period_str == "1 month"


@pytest.fixture(scope="module")
def time_varies():
    """Create data with no standard frequency"""
    np_time = np.array(["2012-02-01", "2012-03-31", "2012-05-21"], dtype="datetime64[ns]")
    time = DataArray(np_time, dims=("time"), coords={"time": np_time})

    return time


def test_infer_period_expect_continuous(time_varies):
    """Check exception when continuous data expected but frequency cannot be inferred from data"""

    with pytest.raises(ValueError):
        infer_date_range(time_varies)


def test_cannot_infer_period_from_frequency(time_varies):
    """Test output when frequency cannot be inferred from data but data need not be continuous"""
    continuous = False
    start_date, end_date, period_str = infer_date_range(time_varies, continuous=continuous)

    assert start_date == timestamp_tzaware("2012-02-01")
    assert end_date == timestamp_tzaware("2012-05-21")
    assert period_str == "varies"


def test_update_zero_dim():
    """
    If "time" coordinate of a Dataset  is 0-dimension, expand dimensions to include time as 1D.
    This will also add "time" as the first dimension for all variables.
    """

    ds = xr.Dataset(
        {"x": np.array(1), "y": ("lat", np.array([1, 2, 3]))},
        coords={"time": np.array(np.datetime64("2014-01-01")), "lat": np.array([10.0, 11.0, 12.0])},
    )

    new_ds = update_zero_dim(ds, dim="time")

    assert new_ds.dims["time"] == 1
    assert new_ds["x"].dims == ("time",)
    assert new_ds["y"].dims == ("time", "lat")  # Decide if this behaviour is what we want


def test_update_zero_dim_no_change():
    """
    If "time" coordinate of a Dataset is already 1-dimension, ensure nothing is updated.
    """

    ds = xr.Dataset(
        {"x": ("time", np.array([1])), "y": (("time", "lat"), np.array([[1, 2, 3]]))},
        coords={"time": np.array([np.datetime64("2014-01-01")]), "lat": np.array([10.0, 11.0, 12.0])},
    )

    new_ds = update_zero_dim(ds, dim="time")

    xr.testing.assert_equal(ds, new_ds)
