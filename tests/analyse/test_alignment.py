import numpy as np
import pandas as pd
import pytest

from openghg.analyse._alignment import (
    _extract_obs_freq,
    infer_freq_in_seconds,
    time_overlap,
    _buffer_start_and_end_dates,
)
from openghg.retrieve import get_obs_surface, get_footprint


@pytest.fixture
def obs_data():
    start_date = "2012-01-01"
    end_date = "2013-01-01"

    site = "tac"
    species = "ch4"
    network = "DECC"
    inlet = "100m"

    obs_surface = get_obs_surface(
        site=site, species=species, start_date=start_date, end_date=end_date, inlet=inlet, network=network
    )
    return obs_surface


@pytest.fixture
def footprint_data():
    start_date = "2012-01-01"
    end_date = "2013-01-01"

    site = "tac"
    domain = "EUROPE"
    inlet = "100m"

    footprint = get_footprint(
        site=site, domain=domain, height=inlet, start_date=start_date, end_date=end_date
    )
    return footprint


def test_extract_obs_freq(obs_data):
    # this obs data has "sampling period" attribute:
    assert _extract_obs_freq(obs_data.data) == 60.0

    # without sampling period, should get None
    del obs_data.data.attrs["sampling_period"]
    assert _extract_obs_freq(obs_data.data) is None


def test_infer_freq_in_seconds():
    """Test that we can infer the frequency from regular data.

    Also check that minor pertubations don't affect the result,
    if they're within tolerance and don't change the median gap.
    """
    freq_in_seconds = 3
    times = pd.date_range("2020-01-01", freq=f"{freq_in_seconds}s", periods=10).values

    assert freq_in_seconds == infer_freq_in_seconds(times)

    # Test that differences within the default tolerance of 1.0s doesn't
    # cause problems. Adding 0.5s to one time point makes two consecutive
    # differences 3.5s and 2.5s, so the total gap between them is 1.0s,
    # which is within tolerance.
    times[3] += pd.Timedelta("0.5s")

    assert freq_in_seconds == infer_freq_in_seconds(times)


def test_infer_freq_in_seconds_tolerance():
    """Test that ValueError is raised if diff between max/min gaps is above tolerance.

    Also test that we can raise tolerance.
    """
    freq_in_seconds = 3
    times = pd.date_range("2020-01-01", freq="3s", periods=10).values

    # adding 1s to a time point gives consecutive differences of 2s and 4s,
    # so the difference between the minimum and maximum gaps is 2s, which is
    # above the default tolerance of 1s
    times[3] += pd.Timedelta("1s")

    with pytest.raises(ValueError):
        infer_freq_in_seconds(times)

    # increasing the tolerance ignores this gap
    assert freq_in_seconds == infer_freq_in_seconds(times, tol=2.0)


def test_infer_freq_footprint():
    """Test the code used to infer the frequency of footprints.

    NOTE: we're not actually testing a function here, just the code
    that is used in another function.
    """
    freq = "1h"
    times = pd.date_range("2020-01-01", freq=freq, periods=10).values
    times[3] += pd.Timedelta("1m")

    # code used to infer freq of "other" (e.g. footprints) in `align_obs_and_other`
    inferred_freq = pd.to_timedelta(np.nanmedian(np.diff(times)))

    assert inferred_freq == pd.to_timedelta(freq)


def test_time_overlap():
    input_start1 = "2020-01-01 01:20:00"
    input_end = "2020-01-01 12:00:00"

    freq1 = pd.to_timedelta("20min")
    times1 = pd.date_range(input_start1, input_end, freq=freq1)

    input_start2 = "2020-01-01 02:00:00"
    freq2 = pd.to_timedelta("1h")
    times2 = pd.date_range(input_start2, input_end, freq=freq2)

    start_date, end_date = time_overlap(times1.values, times2.values, freq1, freq2)
    start1, start2, end = _buffer_start_and_end_dates(start_date, end_date, freq2)

    expected_start1 = pd.to_datetime(input_start2) - pd.to_timedelta(1)
    assert start1 == expected_start1

    expected_start2 = pd.to_datetime(input_start2) - freq2 + pd.to_timedelta(1)
    assert start2 == expected_start2

    # freq1 is shorter, so this will give the earliest end date (since times1 and times2 have same end date)
    expected_end = pd.to_datetime(input_end) - pd.to_timedelta(1) + freq1
    assert end == expected_end
