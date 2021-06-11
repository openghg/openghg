import pytest

from openghg.modules import ObsSurface
from openghg.processing import search
from openghg.objectstore import get_local_bucket
from openghg.util import timestamp_tzaware
from pathlib import Path


def get_datapath(filename, data_type):
    return Path(__file__).resolve(strict=True).parent.joinpath(f"../data/proc_test_data/{data_type}/{filename}")


@pytest.fixture(scope="session", autouse=True)
def data_read():
    get_local_bucket(empty=True)
    network = "DECC"
    bsd_path = get_datapath(filename="bsd.picarro.1minute.248m.dat", data_type="CRDS")
    ObsSurface.read_file(filepath=bsd_path, data_type="CRDS", site="bsd", network=network, inlet="248m")
    hfd_path = get_datapath(filename="hfd.picarro.1minute.100m.min.dat", data_type="CRDS")
    ObsSurface.read_file(filepath=hfd_path, data_type="CRDS", site="hfd", network=network, inlet="100m")
    hfd_path = get_datapath(filename="hfd.picarro.1minute.50m.min.dat", data_type="CRDS")
    ObsSurface.read_file(filepath=hfd_path, data_type="CRDS", site="hfd", network=network, inlet="50m")
    tac_path = get_datapath(filename="tac.picarro.1minute.100m.test.dat", data_type="CRDS")
    ObsSurface.read_file(filepath=tac_path, data_type="CRDS", site="tac", network=network, inlet="100m")

    data_filepath = get_datapath(filename="capegrim-medusa.18.C", data_type="GC")
    prec_filepath = get_datapath(filename="capegrim-medusa.18.precisions.C", data_type="GC")

    ObsSurface.read_file(filepath=(data_filepath, prec_filepath), site="CGO", data_type="GCWERKS", network="AGAGE", inlet=None)


def test_keyword_search():
    results = search(species="co2", site=["bsd"], inlet="248m")

    key = next(iter(results))

    expected_metadata = {
        "site": "bsd",
        "instrument": "picarro",
        "sampling_period": 60,
        "inlet": "248m",
        "port": "8",
        "type": "air",
        "network": "decc",
        "species": "co2",
        "scale": "wmo-x2007",
        "data_type": "timeseries",
    }

    metadata = results[key]["metadata"]

    assert metadata == expected_metadata


def test_search_gc():
    results = search(species=["NF3"], site="CGO")

    key = next(iter(results))

    metadata = results[key]["metadata"]

    expected_metadata = {
        "site": "cgo",
        "instrument": "medusa",
        "species": "nf3",
        "units": "ppt",
        "scale": "sio-12",
        "inlet": "70m",
        "data_type": "timeseries",
        "network": "agage",
        "sampling_period": 1200,
    }

    assert metadata == expected_metadata


def test_location_search():
    # TODO - I feel this test could be improved
    species = ["co2", "ch4"]
    locations = ["hfd", "tac", "bsd"]

    results = search(species=species, locations=locations, find_all=False)

    assert len(results) == 8

    expected_results = [
        ("bsd", "ch4", "248m"),
        ("bsd", "co2", "248m"),
        ("hfd", "ch4", "100m"),
        ("hfd", "ch4", "50m"),
        ("hfd", "co2", "100m"),
        ("hfd", "co2", "50m"),
        ("tac", "ch4", "100m"),
        ("tac", "co2", "100m"),
    ]

    expected_results.sort()

    found_results = []
    for k in results:
        found_results.append((results[k]["metadata"]["site"], results[k]["metadata"]["species"], results[k]["metadata"]["inlet"]))

    found_results.sort()

    assert expected_results == found_results


def test_search_datetimes():
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

    key = next(iter(results))

    expected_metadata = {
        "site": "bsd",
        "instrument": "picarro",
        "sampling_period": 60,
        "inlet": "248m",
        "port": "8",
        "type": "air",
        "network": "decc",
        "species": "co2",
        "scale": "wmo-x2007",
        "data_type": "timeseries",
    }

    metadata = results[key]["metadata"]

    assert len(results[key]["keys"]) == 1
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

    assert len(results[key]["keys"]) == 2


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

    expected_results = [
        ("bsd", "ch4", "248m", "picarro"),
        ("bsd", "co", "248m", "picarro"),
        ("bsd", "co2", "248m", "picarro"),
        ("hfd", "ch4", "100m", "picarro"),
        ("hfd", "ch4", "50m", "picarro"),
        ("hfd", "co", "100m", "picarro"),
        ("hfd", "co", "50m", "picarro"),
        ("hfd", "co2", "100m", "picarro"),
        ("hfd", "co2", "50m", "picarro"),
        ("tac", "ch4", "100m", "picarro"),
        ("tac", "co2", "100m", "picarro"),
    ]

    found_results = []
    for k in results:
        found_results.append(
            (
                results[k]["metadata"]["site"],
                results[k]["metadata"]["species"],
                results[k]["metadata"]["inlet"],
                results[k]["metadata"]["instrument"],
            )
        )

    found_results.sort()

    assert found_results == expected_results

    results = search(find_all=True, species=species, site=sites, inlet=inlet, start_date=start, end_date=end)

    found_results = []
    for k in results:
        found_results.append(
            (
                results[k]["metadata"]["site"],
                results[k]["metadata"]["species"],
                results[k]["metadata"]["inlet"],
                results[k]["metadata"]["instrument"],
            )
        )

    found_results.sort()

    assert found_results == [("bsd", "co2", "248m", "picarro")]


@pytest.mark.skip(reason="Needs update for new keyword arg search")
def test_search_instrument_no_inlet():
    locations = "bsd"
    species = "n2o"
    instrument = "picarro5310"

    results = search(locations=locations, species=species, instrument=instrument)

    expected_keys = ["n2o_bsd_108m_picarro5310", "n2o_bsd_248m_picarro5310"]

    assert sorted(list(results.keys())) == sorted(expected_keys)

    assert len(results["n2o_bsd_108m_picarro5310"]["keys"]) == 1
    assert len(results["n2o_bsd_248m_picarro5310"]["keys"]) == 1

    metadata_108m = {
        "site": "bsd",
        "instrument": "picarro5310",
        "time_resolution": "1_minute",
        "network": "decc",
        "inlet": "108m",
        "port": "2",
        "type": "air",
        "species": "n2o",
        "data_type": "timeseries",
        "scale": "wmo-x2006a",
    }

    metadata_248m = {
        "site": "bsd",
        "instrument": "picarro5310",
        "time_resolution": "1_minute",
        "network": "decc",
        "inlet": "248m",
        "port": "1",
        "type": "air",
        "species": "n2o",
        "data_type": "timeseries",
        "scale": "wmo-x2006a",
    }

    assert results["n2o_bsd_108m_picarro5310"]["metadata"] == metadata_108m
    assert results["n2o_bsd_248m_picarro5310"]["metadata"] == metadata_248m


def test_search_incorrect_inlet_site_finds_nothing():
    locations = "hfd"
    inlet = "3695m"
    species = "CH4"

    results = search(site=locations, species=species, inlet=inlet)

    assert not results


def test_search_nonsense_terms():
    species = ["spam", "eggs", "terry"]
    locations = ["capegrim"]

    results = search(species=species, locations=locations)

    assert not results
