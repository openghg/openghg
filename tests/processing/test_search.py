import pytest

from openghg.processing import search
from openghg.util import timestamp_tzaware
from pandas import Timestamp


def test_specific_keyword_search():
    site = "bsd"
    species = "co2"
    inlet = "248m"
    instrument = "picarro"

    results = search(species=species, site=site, inlet=inlet, instrument=instrument)

    metadata = results.metadata(site=site, species=species, inlet=inlet)

    expected_metadata = {
        "site": "bsd",
        "instrument": "picarro",
        "sampling_period": "60",
        "inlet": "248m",
        "port": "9",
        "type": "air",
        "network": "decc",
        "species": "co2",
        "scale": "wmo-x2007",
        "data_type": "timeseries",
    }

    assert metadata == expected_metadata

    data = results.retrieve(site=site, species=species, inlet="248m")
    ds = data.data

    del ds.attrs["File created"]

    expected_attrs = {
        "data_owner": "Simon O'Doherty",
        "data_owner_email": "s.odoherty@bristol.ac.uk",
        "inlet_height_magl": "248m",
        "comment": "Cavity ring-down measurements. Output from GCWerks",
        "Conditions of use": "Ensure that you contact the data owner at the outset of your project.",
        "Source": "In situ measurements of air",
        "Conventions": "CF-1.6",
        "Processed by": "OpenGHG_Cloud",
        "species": "co2",
        "Calibration_scale": "WMO-X2007",
        "station_longitude": -1.15033,
        "station_latitude": 54.35858,
        "station_long_name": "Bilsdale, UK",
        "station_height_masl": 380.0,
        "site": "bsd",
        "instrument": "picarro",
        "sampling_period": "60",
        "inlet": "248m",
        "port": "9",
        "type": "air",
        "network": "decc",
        "scale": "WMO-X2007",
    }

    assert ds.attrs == expected_attrs


def test_specific_search_gc():
    results = search(species=["NF3"], site="CGO", inlet="70m")

    metadata = results.metadata(site="cgo", species="nf3", inlet="70m")

    expected_metadata = {
        "site": "cgo",
        "instrument": "medusa",
        "species": "nf3",
        "units": "ppt",
        "scale": "sio-12",
        "inlet": "70m",
        "data_type": "timeseries",
        "network": "agage",
        "sampling_period": "1200",
    }

    assert metadata == expected_metadata


def test_unranked_location_search():
    species = ["co2", "ch4"]
    sites = ["hfd", "tac", "bsd"]

    results = search(species=species, site=sites, inlet="100m")

    assert len(results) == 2

    tac_data = results.results["tac"]
    hfd_data = results.results["hfd"]

    assert sorted(list(tac_data.keys())) == ["ch4", "co2"]
    assert sorted(list(hfd_data.keys())) == ["ch4", "co2"]

    with pytest.raises(ValueError):
        tac_co2_keys = results.keys(site="tac", species="co2", inlet="105m")

    tac_co2_keys = results.keys(site="tac", species="co2", inlet="100m")
    tac_ch4_keys = results.keys(site="tac", species="co2", inlet="100m")

    assert len(tac_co2_keys) == 4
    assert len(tac_ch4_keys) == 4

    with pytest.raises(ValueError):
        results.keys(site="bsd", species="co2")

    with pytest.raises(ValueError):
        results.keys(site="bsd", species="ch4")


def test_unranked_search_datetimes():
    species = ["co2"]
    locations = ["bsd"]

    start = timestamp_tzaware("2014-1-1")
    end = timestamp_tzaware("2015-1-1")

    results = search(
        species=species,
        site=locations,
        inlet="248m",
        start_date=start,
        end_date=end,
    )

    metadata = results.metadata(site="bsd", species="co2", inlet="248m")

    expected_metadata = {
        "site": "bsd",
        "instrument": "picarro",
        "sampling_period": "60",
        "inlet": "248m",
        "port": "9",
        "type": "air",
        "network": "decc",
        "species": "co2",
        "scale": "wmo-x2007",
        "data_type": "timeseries",
    }

    data_keys = results.keys(site="bsd", species="co2", inlet="248m")
    assert len(data_keys) == 1
    assert metadata == expected_metadata

    start = timestamp_tzaware("2001-1-1")
    end = timestamp_tzaware("2021-1-1")

    results = search(
        species=species,
        site=locations,
        inlet="248m",
        start_date=start,
        end_date=end,
    )

    data_keys = results.keys(site="bsd", species="co2", inlet="248m")
    assert len(data_keys) == 7


def test_search_find_any_unranked():
    species = ["co2"]
    sites = ["bsd"]
    inlet = "248m"
    instrument = "picarro"

    results = search(find_all=False, species=species, site=sites, inlet=inlet, instrument=instrument)

    raw_results = results.raw()

    assert len(raw_results) == 3

    bsd_expected = ["ch4", "co", "co2"]
    hfd_expected = ["ch4", "co", "co2"]
    tac_expected = ["ch4", "co2"]

    assert sorted(list(raw_results["bsd"].keys())) == bsd_expected
    assert sorted(list(raw_results["hfd"].keys())) == hfd_expected
    assert sorted(list(raw_results["tac"].keys())) == tac_expected

    start = timestamp_tzaware("2014-1-1")
    end = timestamp_tzaware("2015-1-1")

    results = search(
        find_all=False,
        species=species,
        site=sites,
        start_date=start,
        end_date=end,
        inlet=inlet,
        instrument=instrument,
    )

    raw_results = results.raw()

    assert len(raw_results) == 2

    assert sorted(list(raw_results.keys())) == ["bsd", "hfd"]


def test_ranked_bsd_search():
    site = "bsd"
    species = "ch4"

    result = search(site=site, species=species)

    raw_result = result.raw()

    expected_rank_metadata = {
        "2015-01-01-00:00:00+00:00_2015-11-01-00:00:00+00:00": "248m",
        "2014-09-02-00:00:00+00:00_2014-11-01-00:00:00+00:00": "108m",
        "2016-09-02-00:00:00+00:00_2018-11-01-00:00:00+00:00": "108m",
        "2019-01-02-00:00:00+00:00_2021-01-01-00:00:00+00:00": "42m",
    }

    assert expected_rank_metadata == raw_result["bsd"]["ch4"]["rank_metadata"]

    metadata = result.metadata(site="bsd", species="ch4")

    expected_inlet_metadata = {
        "248m": {
            "site": "bsd",
            "instrument": "picarro",
            "sampling_period": "60",
            "inlet": "248m",
            "port": "9",
            "type": "air",
            "network": "decc",
            "species": "ch4",
            "scale": "wmo-x2004a",
            "data_type": "timeseries",
        },
        "108m": {
            "site": "bsd",
            "instrument": "picarro",
            "sampling_period": "60",
            "inlet": "108m",
            "port": "9",
            "type": "air",
            "network": "decc",
            "species": "ch4",
            "scale": "wmo-x2004a",
            "data_type": "timeseries",
        },
        "42m": {
            "site": "bsd",
            "instrument": "picarro",
            "sampling_period": "60",
            "inlet": "42m",
            "port": "9",
            "type": "air",
            "network": "decc",
            "species": "ch4",
            "scale": "wmo-x2004a",
            "data_type": "timeseries",
        },
    }

    assert metadata == expected_inlet_metadata

    obs_data = result.retrieve(site="bsd", species="ch4")

    ch4_data = obs_data.data

    assert ch4_data.time[0] == Timestamp("2014-01-30T11:12:30")
    assert ch4_data.time[-1] == Timestamp("2020-12-01T22:31:30")
    assert ch4_data["ch4"][0] == pytest.approx(1959.55)
    assert ch4_data["ch4"][-1] == pytest.approx(1955.93)
    assert ch4_data["ch4_variability"][0] == 0.79
    assert ch4_data["ch4_variability"][-1] == 0.232
    assert len(ch4_data.time) == 196


# @pytest.mark.skip(reason="Needs update for ranking search")
def test_search_find_any():
    species = ["co2"]
    sites = ["bsd"]
    inlet = "248m"
    instrument = "picarro"

    start = timestamp_tzaware("2014-1-1")
    end = timestamp_tzaware("2015-1-1")

    results = search(
        find_all=False,
        species=species,
        site=sites,
        start_date=start,
        end_date=end,
        inlet=inlet,
        instrument=instrument,
    )

    # print(results)

    raw_results = results.raw()

    bsd_data = raw_results["bsd"]
    hfd_data = raw_results["hfd"]

    expected_bsd_heights = sorted(["248m", "108m", "42m"])

    assert sorted(list(bsd_data["ch4"].keys())) == expected_bsd_heights
    assert sorted(list(bsd_data["co"].keys())) == expected_bsd_heights
    assert sorted(list(bsd_data["co2"].keys())) == expected_bsd_heights

    expected_hfd_heights = ["100m", "50m"]

    assert sorted(list(hfd_data["ch4"].keys())) == expected_hfd_heights
    assert sorted(list(hfd_data["co"].keys())) == expected_hfd_heights
    assert sorted(list(hfd_data["co2"].keys())) == expected_hfd_heights

    assert bsd_data["ch4"]["42m"]["metadata"] == {
        "site": "bsd",
        "instrument": "picarro",
        "sampling_period": "60",
        "inlet": "42m",
        "port": "9",
        "type": "air",
        "network": "decc",
        "species": "ch4",
        "scale": "wmo-x2004a",
        "data_type": "timeseries",
    }

    assert bsd_data["co2"]["42m"]["metadata"] == {
        "site": "bsd",
        "instrument": "picarro",
        "sampling_period": "60",
        "inlet": "42m",
        "port": "9",
        "type": "air",
        "network": "decc",
        "species": "co2",
        "scale": "wmo-x2007",
        "data_type": "timeseries",
    }


def test_search_incorrect_inlet_site_finds_nothing():
    locations = "hfd"
    inlet = "3695m"
    species = "CH4"

    results = search(site=locations, species=species, inlet=inlet)

    assert not results


@pytest.mark.skip(reason="Needs update for new ranking search")
def test_search_nonsense_terms():
    species = ["spam", "eggs", "terry"]
    locations = ["capegrim"]

    results = search(species=species, locations=locations)

    assert not results
