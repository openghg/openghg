import pytest
from pandas import Timestamp, Timedelta
import numpy as np

from openghg.retrieve import get_obs_surface, get_flux, get_footprint


def test_get_obs_surface():
    obsdata = get_obs_surface(site="bsd", species="co2", inlet="248m")
    co2_data = obsdata.data

    assert co2_data.time[0] == Timestamp("2014-01-30T11:12:30")
    assert co2_data.time[-1] == Timestamp("2020-12-01T22:31:30")
    assert co2_data.mf[0] == 409.55
    assert co2_data.mf[-1] == 417.65

    metadata = obsdata.metadata

    assert metadata["data_owner"] == "Simon O'Doherty"
    assert metadata["inlet_height_magl"] == "248m"

    averaged_data = get_obs_surface(site="bsd", species="co2", inlet="248m", average="2h")

    time = obsdata.data.time
    averaged_time = averaged_data.data.time

    assert not time.equals(averaged_time)


def test_no_inlet_no_ranked_data_raises():
    with pytest.raises(ValueError):
        get_obs_surface(site="bsd", species="co2")


def test_get_obs_surface_ranking_single():
    """
    Test data returned from get_obs_surface data
     - ranking data is set
     - inlet is not specified
     - date range should only include date for one inlet
    """

    obsdata = get_obs_surface(site="bsd", species="ch4", start_date="2015-01-01", end_date="2015-11-01")

    data = obsdata.data
    metadata = obsdata.metadata

    assert data
    assert data.attrs["inlet"] == "248m"
    assert metadata["inlet"] == "248m"

    data_at_one_time = data["mf"].sel(time="2015-01-30T11:12:30")
    assert data_at_one_time.size == 1

    # TODO: Untangle this - check this output for the rank_metadata makes sense
    # Full metadata from previous test was
    # assert metadata["rank_metadata"] == {
    #     "2015-01-01-00:00:00+00:00_2015-11-01-00:00:00+00:00": "248m",
    #     "2014-09-02-00:00:00+00:00_2014-11-01-00:00:00+00:00": "108m",
    #     "2016-09-02-00:00:00+00:00_2018-11-01-00:00:00+00:00": "108m",
    #     "2019-01-02-00:00:00+00:00_2021-01-01-00:00:00+00:00": "42m",
    # }
    # In this output this seems to be
    # {'2014-09-02-00:00:00+00:00_2014-11-01-00:00:00+00:00': '108m',
    #  '2016-09-02-00:00:00+00:00_2018-11-01-00:00:00+00:00': '108m',
    #  '2019-01-02-00:00:00+00:00_2021-01-01-00:00:00+00:00': '42m'}
    # Not sure if that is expected


@pytest.mark.xfail(reason="Bug: Not returning unique data - time values duplicated.")
def test_get_obs_surface_ranking_unique():
    """
    Test data returned from get_obs_surface data
     - ranking data is set
     - inlet is not specified
     - date range covers multiple inlets

    Covers tests not included in `test_get_obs_surface_no_inlet_ranking`
    TODO: At the moment this fails - unique data is not returned and there are multiple
    entries for some time stamps. This is a bug which will need to be fixed.
    """
    obsdata = get_obs_surface(site="bsd", species="ch4")

    data = obsdata.data

    inlet_slice = data["inlet"].sel(time=slice("2015-01-01-00:00:00", "2015-11-01-00:00:00")).values
    expected_array = np.tile("248m", len(inlet_slice))

    np.testing.assert_equal(inlet_slice, expected_array)

    data_at_one_time_108m = data["mf"].sel(time="2016-04-02T09:07:30")
    assert data_at_one_time_108m.size == 1
    assert data_at_one_time_108m["inlet"] == "108m"

    data_at_one_time_248m = data.sel(time="2015-01-30T11:12:30")
    assert data_at_one_time_248m["mf"].size == 1
    assert data_at_one_time_248m["inlet"] == "248m"


def test_get_obs_surface_no_inlet_ranking():
    """
    Test metadata and attributes returned from get_obs_surface
     - ranking data is set
     - inlet is not specified
     - date range not specified (all dates returned)

    Checks
     - metadata includes expected "rank_metadata" attribute
     - check inlet details have been appropriately updated
    """
    obsdata = get_obs_surface(site="bsd", species="ch4")

    data = obsdata.data
    metadata = obsdata.metadata

    assert data
    assert metadata["rank_metadata"] == {
        "2015-01-01-00:00:00+00:00_2015-11-01-00:00:00+00:00": "248m",
        "2014-09-02-00:00:00+00:00_2014-11-01-00:00:00+00:00": "108m",
        "2016-09-02-00:00:00+00:00_2018-11-01-00:00:00+00:00": "108m",
        "2019-01-02-00:00:00+00:00_2021-01-01-00:00:00+00:00": "42m",
    }

    assert "inlet" in data
    assert data.attrs["inlet"] == "multiple"
    assert metadata["inlet"] == "multiple"


def test_averaging_incorrect_period_raises():
    with pytest.raises(ValueError):
        get_obs_surface(site="bsd", species="co2", inlet="248m", average="888")


def test_timeslice_slices_correctly():
    # Test time slicing works correctly
    timeslice_data = get_obs_surface(
        site="bsd", species="co2", inlet="248m", start_date="2017-01-01", end_date="2018-03-03"
    )

    sliced_co2_data = timeslice_data.data
    assert sliced_co2_data.time[0] == Timestamp("2017-02-18T06:36:30")
    assert sliced_co2_data.time[-1] == Timestamp("2018-02-18T15:42:30")

@pytest.mark.xfail(reason="Bug: Where's the MHD 10m inlet data?.")
def test_timeslice_slices_correctly_exclusive():
    # Test time slicing works with an exclusive time range for continuous data - up to but not including the end point
    timeslice_data = get_obs_surface(
        site="mhd", species="ch4", inlet="10m", start_date="2012-01-11", end_date="2012-02-05"
    )

    sliced_mhd_data = timeslice_data.data

    sampling_period = Timedelta(75, unit="seconds")

    assert sliced_mhd_data.time[0] == (Timestamp("2012-01-11T00:13") - sampling_period / 2.0)
    assert sliced_mhd_data.time[-1] == (Timestamp("2012-02-04T23:47") - sampling_period / 2.0)
    assert sliced_mhd_data.mf[0] == 1849.814
    assert sliced_mhd_data.mf[-1] == 1891.094


def test_get_flux():
    flux_data = get_flux(species="co2", sources="gpp-cardamom", domain="europe")

    flux = flux_data.data

    assert float(flux.lat.max()) == pytest.approx(79.057)
    assert float(flux.lat.min()) == pytest.approx(10.729)
    assert float(flux.lon.max()) == pytest.approx(39.38)
    assert float(flux.lon.min()) == pytest.approx(-97.9)
    assert sorted(list(flux.variables)) == ["flux", "lat", "lon", "time"]
    assert flux.attrs["species"] == "co2"


def test_get_flux_no_result():
    with pytest.raises(ValueError):
        get_flux(species="co2", sources="cinnamon", domain="antarctica")


def test_get_footprint():
    fp_result = get_footprint(site="tmb", domain="europe", height="10m", model="test_model")

    footprint = fp_result.data
    metadata = fp_result.metadata

    assert footprint.time[0] == Timestamp("2020-08-01")
    assert footprint.time[-1] == Timestamp("2020-08-01")

    assert metadata["max_longitude"] == pytest.approx(float(footprint.lon.max()))
    assert metadata["min_longitude"] == pytest.approx(float(footprint.lon.min()))
    assert metadata["max_latitude"] == pytest.approx(float(footprint.lat.max()))
    assert metadata["min_latitude"] == pytest.approx(float(footprint.lat.min()))
    assert metadata["time_resolution"] == "standard_time_resolution"


def test_get_footprint_no_result():
    with pytest.raises(ValueError):
        get_footprint(site="seville", domain="spain", height="10m", model="test_model")
