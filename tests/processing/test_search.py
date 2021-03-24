import os
import pytest

from openghg.modules import ObsSurface
from openghg.processing import search
from openghg.objectstore import get_local_bucket
from openghg.util import timestamp_tzaware


@pytest.fixture(scope="session")
def gc_read():
    get_local_bucket(empty=True)
    data_file = "capegrim-medusa.18.C"
    prec_file = "capegrim-medusa.18.precisions.C"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/GC"
    data_filepath = os.path.join(dir_path, test_data, data_file)
    prec_filepath = os.path.join(dir_path, test_data, prec_file)

    ObsSurface.read_file(filepath=(data_filepath, prec_filepath), data_type="GCWERKS", network="AGAGE")


@pytest.fixture(scope="session")
def crds_read():
    get_local_bucket(empty=True)
    test_data = "../data/search_data"
    folder_path = os.path.join(os.path.dirname(__file__), test_data)
    ObsSurface.read_folder(folder_path=folder_path, data_type="CRDS", network="DECC", extension="dat")


def test_search_gc(gc_read):
    results = search(species=["NF3"], locations="capegrim")

    nf3_results = results["nf3_cgo_75m_4_medusa"]

    metadata = {
        "site": "cgo",
        "instrument": "medusa",
        "species": "nf3",
        "units": "ppt",
        "scale": "sio-12",
        "inlet": "75m_4",
        "data_type": "timeseries",
        "network": "agage",
    }

    assert "2018-01-01-02:24:00+00:00_2018-01-31-23:33:00+00:00" in nf3_results["keys"][0]
    assert nf3_results["metadata"] == metadata


def test_location_search(crds_read):
    species = ["co2", "ch4"]
    locations = ["hfd", "tac", "bsd"]

    results = search(species=species, locations=locations, find_all=False)

    results_list = sorted(list(results.keys()))

    expected = sorted(
        [
            "ch4_bsd_108m_picarro",
            "ch4_bsd_248m_picarro",
            "ch4_hfd_100m_picarro",
            "ch4_tac_100m_picarro",
            "co2_bsd_108m_picarro",
            "co2_bsd_248m_picarro",
            "co2_hfd_100m_picarro",
            "co2_tac_100m_picarro",
        ]
    )

    assert results_list == expected

    assert len(results["co2_bsd_108m_picarro"]["keys"]) == 2
    assert len(results["co2_hfd_100m_picarro"]["keys"]) == 1
    assert len(results["co2_tac_100m_picarro"]["keys"]) == 1
    assert len(results["ch4_bsd_108m_picarro"]["keys"]) == 2
    assert len(results["ch4_hfd_100m_picarro"]["keys"]) == 1
    assert len(results["ch4_tac_100m_picarro"]["keys"]) == 1


def test_search_datetimes(crds_read):
    species = ["co2"]
    locations = ["bsd"]

    start = timestamp_tzaware("2014-1-1")
    end = timestamp_tzaware("2015-1-1")

    results = search(
        species=species,
        locations=locations,
        find_all=False,
        start_date=start,
        end_date=end,
    )

    expected_keys = ["co2_bsd_248m_picarro", "co2_bsd_108m_picarro"]
    assert list(results.keys()) == expected_keys

    bsd_108_keys = results["co2_bsd_108m_picarro"]["keys"]
    bsd_248_keys = results["co2_bsd_248m_picarro"]["keys"]

    bsd_108_date_strings = [v.split("/")[-1] for v in bsd_108_keys]
    bsd_248_date_strings = [v.split("/")[-1] for v in bsd_248_keys]

    assert bsd_108_date_strings == ["2014-01-30-13:33:30+00:00_2014-01-31-11:02:30+00:00"]
    assert bsd_248_date_strings == ["2014-01-30-10:52:30+00:00_2014-01-30-14:20:30+00:00"]

    metadata_108 = results["co2_bsd_108m_picarro"]["metadata"]
    metadata_248 = results["co2_bsd_248m_picarro"]["metadata"]

    expected_metadata_108 = {
        "site": "bsd",
        "instrument": "picarro",
        "time_resolution": "1_minute",
        "inlet": "108m",
        "port": "9",
        "type": "air",
        "species": "co2",
        "data_type": "timeseries",
        "scale": "wmo-x2007",
        "network": "decc",
    }

    expected_metadata_248 = {
        "site": "bsd",
        "instrument": "picarro",
        "time_resolution": "1_minute",
        "inlet": "248m",
        "port": "8",
        "type": "air",
        "network": "decc",
        "species": "co2",
        "scale": "wmo-x2007",
        "data_type": "timeseries",
    }

    assert metadata_108 == expected_metadata_108
    assert metadata_248 == expected_metadata_248


def test_search_find_all():
    species = ["co2"]
    locations = ["bsd"]
    inlet = "108m"
    instrument = "picarro"

    start = timestamp_tzaware("2014-1-1")
    end = timestamp_tzaware("2015-1-1")

    results = search(
        species=species, locations=locations, find_all=True, start_date=start, end_date=end, inlet=inlet, instrument=instrument
    )

    bsd_results = results["co2_bsd_108m_picarro"]

    assert bsd_results["metadata"]["site"] == "bsd"
    assert bsd_results["metadata"]["species"] == "co2"
    assert bsd_results["metadata"]["time_resolution"] == "1_minute"


    key_dates = [daterange.split("/")[-1] for daterange in bsd_results["keys"]]

    assert key_dates == ['2014-01-30-13:33:30+00:00_2014-01-31-11:02:30+00:00']


def test_search_no_species(crds_read):
    locations = "bsd"

    results = search(locations=locations)

    expected_keys = sorted(
        [
            "ch4_bsd_248m_picarro",
            "co2_bsd_248m_picarro",
            "co_bsd_248m_picarro",
            "co_bsd_108m_picarro5310",
            "n2o_bsd_108m_picarro5310",
            "ch4_bsd_108m_picarro",
            "co2_bsd_108m_picarro",
            "co_bsd_108m_picarro",
            "co_bsd_248m_picarro5310",
            "n2o_bsd_248m_picarro5310",
        ]
    )

    assert sorted(list(results.keys())) == expected_keys


def test_search_with_inlet_instrument(crds_read):
    locations = "hfd"
    inlet = "100m"
    instrument = "picarro"
    species = "CH4"

    results = search(locations=locations, species=species, inlet=inlet, instrument=instrument)

    assert len(results["ch4_hfd_100m_picarro"]["keys"]) == 1

    expected_metadata = {
        "site": "hfd",
        "instrument": "picarro",
        "time_resolution": "1_minute",
        "scale": "wmo-x2004a",
        "inlet": "100m",
        "port": "10",
        "type": "air",
        "species": "ch4",
        "data_type": "timeseries",
        "network": "decc",
    }

    assert results["ch4_hfd_100m_picarro"]["metadata"] == expected_metadata


def test_search_inlet_no_instrument(crds_read):
    locations = "hfd"
    inlet = "100m"
    species = "CH4"

    results = search(locations=locations, species=species, inlet=inlet)

    expected_keys = ["ch4_hfd_100m_picarro"]

    assert list(results.keys()) == expected_keys

    assert len(results["ch4_hfd_100m_picarro"]["keys"]) == 1

    expected_metadata = {
        "site": "hfd",
        "instrument": "picarro",
        "time_resolution": "1_minute",
        "inlet": "100m",
        "port": "10",
        "type": "air",
        "species": "ch4",
        "data_type": "timeseries",
        "scale": "wmo-x2004a",
        "network": "decc",
    }

    assert results["ch4_hfd_100m_picarro"]["metadata"] == expected_metadata


def test_search_instrument_no_inlet(crds_read):
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


def test_search_incorrect_inlet_site_finds_nothing(crds_read):
    locations = "hfd"
    inlet = "3695m"
    species = "CH4"

    results = search(locations=locations, species=species, inlet=inlet)

    assert not results


def test_search_bad_site_raises():
    species = ["spam", "eggs", "terry"]
    locations = ["tintagel"]

    with pytest.raises(ValueError):
        search(species=species, locations=locations)


def test_search_nonsense_terms():
    species = ["spam", "eggs", "terry"]
    locations = ["capegrim"]

    results = search(species=species, locations=locations)

    assert not results
