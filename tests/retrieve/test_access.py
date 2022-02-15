import pytest
from pandas import Timestamp, Timedelta
import numpy as np

from openghg.retrieve import get_obs_surface, get_flux, get_footprint, search
from openghg.objectstore import get_local_bucket

import pytest
from openghg.objectstore import get_local_bucket
from openghg.store import ObsSurface, Emissions, Footprints
from helpers import (
    get_datapath,
    get_emissions_datapath,
    get_footprint_datapath,
    metadata_checker_obssurface,
    attributes_checker_get_obs,
)


a = [
    "1970-01-01-00:00:00+00:00_2011-12-31-00:00:00+00:00",
    "2013-01-02-00:00:00+00:00_2014-09-01-00:00:00+00:00",
    "2014-11-02-00:00:00+00:00_2014-12-31-00:00:00+00:00",
    "2015-11-02-00:00:00+00:00_2016-09-01-00:00:00+00:00",
    "2018-11-02-00:00:00+00:00_2019-01-01-00:00:00+00:00",
    "2021-01-02-00:00:00+00:00_2022-02-09-15:55:27.361446+00:00",
]

b = {
    "2015-01-01-00:00:00+00:00_2015-11-01-00:00:00+00:00": "248m",
    "2019-01-02-00:00:00+00:00_2021-01-01-00:00:00+00:00": "42m",
    "2014-09-02-00:00:00+00:00_2014-11-01-00:00:00+00:00": "108m",
    "2016-09-02-00:00:00+00:00_2018-11-01-00:00:00+00:00": "108m",
}

c = [
    "2015-01-01-00:00:00+00:00_2015-11-01-00:00:00+00:00",
    "2019-01-02-00:00:00+00:00_2021-01-01-00:00:00+00:00",
    "2014-09-02-00:00:00+00:00_2014-11-01-00:00:00+00:00",
    "2016-09-02-00:00:00+00:00_2018-11-01-00:00:00+00:00",
]


# def test_no_duplicate_timestamps():
#     get_local_bucket(empty=True)

#     # DECC network sites
#     network = "DECC"
#     bsd_248_path = get_datapath(filename="bsd.picarro.1minute.248m.min.dat", data_type="CRDS")
#     bsd_108_path = get_datapath(filename="bsd.picarro.1minute.108m.min.dat", data_type="CRDS")
#     bsd_42_path = get_datapath(filename="bsd.picarro.1minute.42m.min.dat", data_type="CRDS")

#     bsd_paths = [bsd_248_path, bsd_108_path, bsd_42_path]

#     bsd_results = ObsSurface.read_file(filepath=bsd_paths, data_type="CRDS", site="bsd", network=network)

#     obs = ObsSurface.load()

#     uid_248 = bsd_results["processed"]["bsd.picarro.1minute.248m.min.dat"]["ch4"]
#     obs.set_rank(uuid=uid_248, rank=1, date_range="2014-01-30_2015-01-01")

#     uid_108 = bsd_results["processed"]["bsd.picarro.1minute.108m.min.dat"]["ch4"]
#     obs.set_rank(uuid=uid_108, rank=1, date_range="2015-01-02_2015-11-01")

#     obs.set_rank(uuid=uid_248, rank=1, date_range="2016-04-01_2017-11-01")

#     uid_42 = bsd_results["processed"]["bsd.picarro.1minute.42m.min.dat"]["ch4"]
#     obs.set_rank(uuid=uid_42, rank=1, date_range="2019-01-01_2021-01-01")

#     obs.save()

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
#             "2015-11-02-00:00:00+00:00_2016-03-31-00:00:00+00:00": "248m",
#             "2017-11-02-00:00:00+00:00_2018-12-31-00:00:00+00:00": "248m",
#         },
#     }

#     assert metadata["rank_metadata"] == expected_rank_metadata


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


def test_get_obs_surface_ranked_data_only():
    obsdata = get_obs_surface(site="bsd", species="ch4", start_date="2014-02-01", end_date="2014-12-31")
    metadata = obsdata.metadata

    assert metadata["rank_metadata"] == {
        "ranked": {"2014-01-30-00:00:00+00:00_2015-01-01-00:00:00+00:00": "248m"}
    }


def test_get_obs_surface_no_ranked_data_raises_until_search_narrowed():
    with pytest.raises(ValueError):
        get_obs_surface(site="bsd", species="ch4", start_date="2018-02-01", end_date="2018-12-31")

    obsdata = get_obs_surface(
        site="bsd", species="ch4", inlet="42m", start_date="2018-02-01", end_date="2018-12-31"
    )

    attrs = obsdata.data.attrs
    metadata = obsdata.metadata

    metadata_checker_obssurface(metadata=metadata, species="ch4")
    attributes_checker_get_obs(attrs=attrs, species="ch4")


def test_get_obs_surface_ranking_single():
    """
    Test data returned from get_obs_surface data
     - ranking data is set
     - inlet is not specified
     - date range should only include date for one inlet
    """

    obsdata = get_obs_surface(site="bsd", species="ch4", start_date="2015-01-01", end_date="2015-11-01")

    print(obsdata)

    print(obsdata.data["inlet"])

    return

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
    res = search(site="bsd", species="ch4")

    data = res.retrieve(site="bsd", species="ch4")

    metadata = data.metadata

    expected_rank_metadata = {
        "ranked": {
            "2014-01-30-00:00:00+00:00_2015-01-01-00:00:00+00:00": "248m",
            "2016-04-01-00:00:00+00:00_2017-11-01-00:00:00+00:00": "248m",
            "2015-01-02-00:00:00+00:00_2015-11-01-00:00:00+00:00": "108m",
            "2019-01-01-00:00:00+00:00_2021-01-01-00:00:00+00:00": "42m",
        },
        "unranked": {
            "2015-11-02-00:00:00+00:00_2016-03-31-00:00:00+00:00": "248m",
            "2017-11-02-00:00:00+00:00_2018-12-31-00:00:00+00:00": "248m",
        },
    }

    assert metadata["rank_metadata"] == expected_rank_metadata


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
