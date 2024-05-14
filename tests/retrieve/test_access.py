import datetime

import numpy as np
import pandas as pd
import pytest
from helpers import call_function_packager
from openghg.dataobjects import ObsData
from openghg.retrieve import (
    get_bc,
    get_flux,
    get_footprint,
    get_obs_column,
    get_obs_surface,
    search,
)
from openghg.types import SearchError
from openghg.util import compress, compress_str, hash_bytes
from pandas import Timedelta, Timestamp

# a = [
#     "1970-01-01-00:00:00+00:00_2011-12-31-00:00:00+00:00",
#     "2013-01-02-00:00:00+00:00_2014-09-01-00:00:00+00:00",
#     "2014-11-02-00:00:00+00:00_2014-12-31-00:00:00+00:00",
#     "2015-11-02-00:00:00+00:00_2016-09-01-00:00:00+00:00",
#     "2018-11-02-00:00:00+00:00_2019-01-01-00:00:00+00:00",
#     "2021-01-02-00:00:00+00:00_2022-02-09-15:55:27.361446+00:00",
# ]

# b = {
#     "2015-01-01-00:00:00+00:00_2015-11-01-00:00:00+00:00": "248m",
#     "2019-01-02-00:00:00+00:00_2021-01-01-00:00:00+00:00": "42m",
#     "2014-09-02-00:00:00+00:00_2014-11-01-00:00:00+00:00": "108m",
#     "2016-09-02-00:00:00+00:00_2018-11-01-00:00:00+00:00": "108m",
# }

# c = [
#     "2015-01-01-00:00:00+00:00_2015-11-01-00:00:00+00:00",
#     "2019-01-02-00:00:00+00:00_2021-01-01-00:00:00+00:00",
#     "2014-09-02-00:00:00+00:00_2014-11-01-00:00:00+00:00",
#     "2016-09-02-00:00:00+00:00_2018-11-01-00:00:00+00:00",
# ]


def test_get_obs_surface_average_works_without_longname():
    # The stored dataset here doesn't have a long_name attribute so failed
    # Now works with checks
    obsdata = get_obs_surface(
        site="mhd",
        species="ch4",
        inlet="10magl",
        average="4H",
        instrument="gcmd",
    )

    assert obsdata.data.attrs["averaged_period_str"] == "4H"
    assert obsdata.data.attrs["averaged_period"] == 14400


@pytest.mark.parametrize(
    "inlet_keyword,inlet_value",
    [
        ("inlet", "248m"),
        ("height", "248m"),
        ("inlet", "248magl"),
        ("inlet", "248"),
    ],
)
def test_get_obs_surface(inlet_keyword, inlet_value):
    if inlet_keyword == "inlet":
        obsdata = get_obs_surface(site="bsd", species="co2", inlet=inlet_value)
    elif inlet_keyword == "height":
        obsdata = get_obs_surface(site="bsd", species="co2", height=inlet_value)
    co2_data = obsdata.data

    assert co2_data.time[0] == Timestamp("2014-01-30T11:12:30")
    assert co2_data.time[-1] == Timestamp("2020-12-01T22:31:30")
    assert co2_data.mf[0] == 409.55
    assert co2_data.mf[-1] == 417.65

    metadata = obsdata.metadata

    assert metadata["data_owner"] == "Simon O'Doherty"
    assert metadata["inlet"] == "248m"
    assert metadata["inlet_height_magl"] == "248"

    averaged_data = get_obs_surface(site="bsd", species="co2", inlet="248m", average="2h")

    time = obsdata.data.time
    averaged_time = averaged_data.data.time

    assert not time.equals(averaged_time)


def test_ambiguous_no_ranked_data_raises():
    """
    Test sensible error message is raised when result is ambiguous for
    get_obs_surface() function
    """
    with pytest.raises(SearchError) as excinfo:
        get_obs_surface(site="bsd", species="co2")
        assert "Multiple entries found for input parameters" in excinfo


def test_no_data_raises():
    """
    Test sensible error message is raised when no data can be found
    get_obs_surface() function
    """
    with pytest.raises(SearchError) as excinfo:
        site = "bsd"
        species = "cfc11"
        get_obs_surface(site=site, species=species)

        assert "Unable to find results for" in excinfo
        assert f"site='{site}'" in excinfo
        assert f"species='{species}'" in excinfo


# def test_get_obs_surface_ranked_data_only():
#     obsdata = get_obs_surface(site="bsd", species="ch4", start_date="2014-02-01", end_date="2014-12-31")
#     metadata = obsdata.metadata

#     assert metadata["rank_metadata"] == {
#         "ranked": {"2014-01-30-00:00:00+00:00_2015-01-01-00:00:00+00:00": "248m"}
#     }


# def test_get_obs_surface_no_ranked_data_raises_until_search_narrowed():
#     with pytest.raises(ValueError):
#         get_obs_surface(site="bsd", species="ch4", start_date="2018-02-01", end_date="2018-12-31")

#     obsdata = get_obs_surface(
#         site="bsd", species="ch4", inlet="42m", start_date="2018-02-01", end_date="2018-12-31"
#     )

#     attrs = obsdata.data.attrs
#     metadata = obsdata.metadata

#     metadata_checker_obssurface(metadata=metadata, species="ch4")
#     attributes_checker_get_obs(attrs=attrs, species="ch4")


# def test_get_obs_surface_ranking_single():
#     """
#     Test data returned from get_obs_surface data
#      - ranking data is set
#      - inlet is not specified
#      - date range should only include date for one inlet
#     """
#     obsdata = get_obs_surface(site="bsd", species="ch4", start_date="2015-01-10", end_date="2015-11-01")

#     data = obsdata.data
#     metadata = obsdata.metadata

#     assert data
#     assert data.attrs["inlet"] == "108m"
#     assert metadata["inlet"] == "108m"

#     assert metadata["rank_metadata"] == {
#         "ranked": {"2015-01-02-00:00:00+00:00_2015-11-01-00:00:00+00:00": "108m"}
#     }

#     # Make sure we don't have duplicate timestamps
#     data_at_one_time = data["mf"].sel(time="2015-01-30T11:12:30")
#     assert data_at_one_time.size == 1


# def test_get_obs_surface_ranking_unique():
#     """
#     Test data returned from get_obs_surface data
#      - ranking data is set
#      - inlet is not specified
#      - date range covers multiple inlets

#     Covers tests not included in `test_get_obs_surface_no_inlet_ranking`
#     TODO: At the moment this fails - unique data is not returned and there are multiple
#     entries for some time stamps. This is a bug which will need to be fixed.
#     """
#     res = search(site="bsd", species="ch4")

#     data = res.retrieve(site="bsd", species="ch4")

#     metadata = data.metadata

#     expected_rank_metadata = {
#         "ranked": {
#             "2014-01-30-00:00:00+00:00_2015-01-01-00:00:00+00:00": "248m",
#             "2016-04-01-00:00:00+00:00_2017-11-01-00:00:00+00:00": "248m",
#             "2015-01-02-00:00:00+00:00_2015-11-01-00:00:00+00:00": "108m",
#             "2019-01-01-00:00:00+00:00_2021-01-01-00:00:00+00:00": "42m",
#         },
#         "unranked": {
#             "2015-11-01-01:00:00+00:00_2016-03-31-23:00:00+00:00": "248m",
#             "2017-11-01-01:00:00+00:00_2018-12-31-23:00:00+00:00": "248m",
#         },
#     }

#     assert metadata["rank_metadata"] == expected_rank_metadata


# def test_get_obs_surface_no_inlet_ranking():
#     """
#     Test metadata and attributes returned from get_obs_surface
#      - ranking data is set
#      - inlet is not specified
#      - date range not specified (all dates returned)

#     Checks
#      - metadata includes expected "rank_metadata" attribute
#      - check inlet details have been appropriately updated
#     """
#     obsdata = get_obs_surface(site="bsd", species="ch4")

#     data = obsdata.data
#     metadata = obsdata.metadata

#     assert data

#     expeced_rank_metadata = {
#         "ranked": {
#             "2014-01-30-00:00:00+00:00_2015-01-01-00:00:00+00:00": "248m",
#             "2016-04-01-00:00:00+00:00_2017-11-01-00:00:00+00:00": "248m",
#             "2015-01-02-00:00:00+00:00_2015-11-01-00:00:00+00:00": "108m",
#             "2019-01-01-00:00:00+00:00_2021-01-01-00:00:00+00:00": "42m",
#         },
#         "unranked": {
#             "2015-11-01-01:00:00+00:00_2016-03-31-23:00:00+00:00": "248m",
#             "2017-11-01-01:00:00+00:00_2018-12-31-23:00:00+00:00": "248m",
#         },
#     }

#     assert metadata["rank_metadata"] == expeced_rank_metadata

#     assert "inlet" in data
#     assert data.attrs["inlet"] == "multiple"
#     assert metadata["inlet"] == "multiple"


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

    sampling_period = Timedelta(1, unit="seconds")

    assert sliced_mhd_data.time[0] == (Timestamp("2012-01-11T00:13") - sampling_period / 2.0)
    assert sliced_mhd_data.time[-1] == (Timestamp("2012-02-04T23:47") - sampling_period / 2.0)
    assert sliced_mhd_data.mf[0] == 1849.814
    assert sliced_mhd_data.mf[-1] == 1891.094

@pytest.mark.xfail(reason="Mark this for removal. Our cloud functions will need an overhaul.")
def test_get_obs_surface_cloud(mocker, monkeypatch):
    monkeypatch.setenv("OPENGHG_HUB", "1")

    n_days = 100
    epoch = datetime.datetime(1970, 1, 1, 1, 1)

    mock_dataset = pd.DataFrame(
        data={"A": range(0, n_days)},
        index=pd.date_range(epoch, epoch + datetime.timedelta(n_days - 1), freq="D"),
    ).to_xarray()

    mock_meta = {"some": "metadata"}
    mock_obs = ObsData(data=mock_dataset, metadata=mock_meta)

    for_transfer = mock_obs.to_data()

    sha1_hash = hash_bytes(data=for_transfer["data"])

    to_return = {
        "found": True,
        "data": compress(data=for_transfer["data"]),
        "metadata": compress_str(s=for_transfer["metadata"]),
        "file_metadata": {
            "data": {"sha1_hash": sha1_hash, "compression_type": "gzip"},
            "metadata": {"sha1_hash": False, "compression_type": "bz2"},
        },
    }

    to_return = call_function_packager(status=200, headers={}, content=to_return)

    mocker.patch("openghg.cloud.call_function", return_value=to_return)

    result = get_obs_surface(site="london", species="hawk")

    assert result == mock_obs


def test_get_obs_column():
    column_data = get_obs_column(species="ch4", satellite="gosat")

    obscolumn = column_data.data

    assert "xch4" in obscolumn
    assert obscolumn.time[0] == Timestamp("2017-03-18T15:32:54")
    assert np.isclose(obscolumn["xch4"][0], 1844.2019)
    assert obscolumn.attrs["species"] == "ch4"


def test_get_flux():
    flux_data = get_flux(species="co2", source="gpp-cardamom", domain="europe")

    flux = flux_data.data

    assert float(flux.lat.max()) == pytest.approx(79.057)
    assert float(flux.lat.min()) == pytest.approx(10.729)
    assert float(flux.lon.max()) == pytest.approx(39.38)
    assert float(flux.lon.min()) == pytest.approx(-97.9)
    assert sorted(list(flux.variables)) == ["flux", "lat", "lon", "time"]

    # Check whole flux range has been retrieved (2 files)
    time = flux["time"]
    assert time[0] == Timestamp("2012-01-01T00:00:00")
    assert time[-1] == Timestamp("2013-01-01T00:00:00")


def test_get_flux_range():
    """Test data can be retrieved with a start and end date range when data is added non-sequentially (check conftest.py)"""
    flux_data = get_flux(species="co2", source="gpp-cardamom", domain="europe", start_date="2012-01-01", end_date="2012-05-01")

    flux = flux_data.data

    # Check a single time value has been retrieved
    time = flux["time"]
    assert len(time) == 1
    assert time[0] == Timestamp("2012-01-01T00:00:00")


def test_get_flux_no_result():
    """Test sensible error message is being returned when no results are found
    with input keywords for get_flux function"""
    with pytest.raises(SearchError) as execinfo:
        get_flux(species="co2", source="cinnamon", domain="antarctica")
        assert "Unable to find results" in execinfo
        assert "species='co2'" in execinfo
        assert "source='cinnamon'" in execinfo
        assert "domain='antarctica'" in execinfo


def test_get_bc():
    bc_data = get_bc(species="n2o", bc_input="mozart", domain="europe")

    bc = bc_data.data

    assert float(bc.lat.max()) == pytest.approx(79.057)
    assert float(bc.lat.min()) == pytest.approx(10.729)
    assert float(bc.lon.max()) == pytest.approx(39.38)
    assert float(bc.lon.min()) == pytest.approx(-97.9)

    bc_variables = ["height", "lat", "lon", "time", "vmr_e", "vmr_n", "vmr_s", "vmr_w"]
    assert sorted(list(bc.variables)) == bc_variables

    time = bc["time"]
    assert time[0] == Timestamp("2012-01-01T00:00:00")


@pytest.mark.parametrize(
    "inlet_keyword,inlet_value",
    [
        ("inlet", "10m"),
        ("height", "10m"),
        ("inlet", "10magl"),
        ("inlet", "10"),
    ],
)
def test_get_footprint(inlet_keyword, inlet_value):
    if inlet_keyword == "inlet":
        fp_result = get_footprint(site="tmb", domain="europe", inlet=inlet_value, model="test_model")
    elif inlet_keyword == "height":
        fp_result = get_footprint(site="tmb", domain="europe", height=inlet_value, model="test_model")

    footprint = fp_result.data
    metadata = fp_result.metadata

    assert footprint.time[0] == Timestamp("2020-08-01")
    assert footprint.time[-1] == Timestamp("2020-08-01")

    assert metadata["max_longitude"] == pytest.approx(float(footprint["lon"].max()))
    assert metadata["min_longitude"] == pytest.approx(float(footprint["lon"].min()))
    assert metadata["max_latitude"] == pytest.approx(float(footprint["lat"].max()))
    assert metadata["min_latitude"] == pytest.approx(float(footprint["lat"].min()))
    assert metadata["time_resolved"] == "false"


def test_get_footprint_no_result():
    """Test sensible error message is being returned when no results are found
    with input keywords for get_footprint function"""
    with pytest.raises(SearchError) as execinfo:
        get_footprint(site="seville", domain="spain", height="10m", model="test_model")
        assert "Unable to find results" in execinfo
        assert "site='seville'" in execinfo
        assert "domain='spain'" in execinfo
        assert "height='10m'" in execinfo
        assert "model='test_model'" in execinfo
