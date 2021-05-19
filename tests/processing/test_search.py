import pytest

from openghg.modules import ObsSurface
from openghg.processing import search
from openghg.objectstore import get_local_bucket
from openghg.util import timestamp_tzaware
from pandas import Timestamp

from helpers import get_datapath


@pytest.fixture(scope="session", autouse=True)
def data_read():
    get_local_bucket(empty=True)
    network = "DECC"
    bsd_248_path = get_datapath(filename="bsd.picarro.1minute.248m.min.dat", data_type="CRDS")
    bsd_108_path = get_datapath(filename="bsd.picarro.1minute.108m.min.dat", data_type="CRDS")
    bsd_42_path = get_datapath(filename="bsd.picarro.1minute.42m.min.dat", data_type="CRDS")

    bsd_paths = [bsd_248_path, bsd_108_path, bsd_42_path]

    bsd_results = ObsSurface.read_file(filepath=bsd_paths, data_type="CRDS", site="bsd", network=network)

    hfd_100_path = get_datapath(filename="hfd.picarro.1minute.100m.min.dat", data_type="CRDS")
    hfd_50_path = get_datapath(filename="hfd.picarro.1minute.50m.min.dat", data_type="CRDS")
    hfd_paths = [hfd_100_path, hfd_50_path]

    ObsSurface.read_file(filepath=hfd_paths, data_type="CRDS", site="hfd", network=network)

    tac_path = get_datapath(filename="tac.picarro.1minute.100m.test.dat", data_type="CRDS")
    ObsSurface.read_file(filepath=tac_path, data_type="CRDS", site="tac", network=network)

    data_filepath = get_datapath(filename="capegrim-medusa.18.C", data_type="GC")
    prec_filepath = get_datapath(filename="capegrim-medusa.18.precisions.C", data_type="GC")

    ObsSurface.read_file(filepath=(data_filepath, prec_filepath), site="CGO", data_type="GCWERKS", network="AGAGE")

    obs = ObsSurface.load()

    uid_248 = bsd_results["processed"]["bsd.picarro.1minute.248m.min.dat"]["ch4"]
    obs.set_rank(uuid=uid_248, rank=1, date_range="2012-01-01_2013-01-01")

    uid_108 = bsd_results["processed"]["bsd.picarro.1minute.108m.min.dat"]["ch4"]
    obs.set_rank(uuid=uid_108, rank=1, date_range="2014-09-02_2014-11-01")

    obs.set_rank(uuid=uid_248, rank=1, date_range="2015-01-01_2015-11-01")

    obs.set_rank(uuid=uid_108, rank=1, date_range="2016-09-02_2018-11-01")

    uid_42 = bsd_results["processed"]["bsd.picarro.1minute.42m.min.dat"]["ch4"]
    obs.set_rank(uuid=uid_42, rank=1, date_range="2019-01-02_2021-01-01")


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
        tac_co2_keys = results.keys(site="tac", species="co2")

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
        find_all=False, species=species, site=sites, start_date=start, end_date=end, inlet=inlet, instrument=instrument
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
        "2015-01-01_2015-11-01": "248m",
        "2014-09-02_2014-11-01": "108m",
        "2016-09-02_2018-11-01": "108m",
        "2019-01-02_2021-01-01": "42m",
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
        find_all=False, species=species, site=sites, start_date=start, end_date=end, inlet=inlet, instrument=instrument
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
