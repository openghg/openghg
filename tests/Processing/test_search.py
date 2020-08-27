import os
import pytest

from HUGS.Modules import ObsSurface
from HUGS.Processing import search
from HUGS.ObjectStore import get_local_bucket
from HUGS.Util import get_datetime


@pytest.fixture(scope="session")
def gc_read():
    get_local_bucket(empty=True)
    data_file = "capegrim-medusa.18.C"
    prec_file = "capegrim-medusa.18.precisions.C"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/GC"
    data_filepath = os.path.join(dir_path, test_data, data_file)
    prec_filepath = os.path.join(dir_path, test_data, prec_file)

    ObsSurface.read_file(filepath=(data_filepath, prec_filepath), data_type="GC")


@pytest.fixture(scope="session")
def crds_read():
    get_local_bucket(empty=True)
    test_data = "../data/search_data"
    folder_path = os.path.join(os.path.dirname(__file__), test_data)
    ObsSurface.read_folder(folder_path=folder_path, data_type="CRDS", extension="dat")


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
    }

    assert "2018-01-01-02:24:00_2018-01-31-23:33:00" in nf3_results["keys"]
    assert nf3_results["metadata"] == metadata


def test_location_search(crds_read):
    species = ["co2", "ch4"]
    locations = ["bsd", "hfd", "tac"]

    start = None  # get_datetime(year=2016, month=1, day=1)
    end = None  # get_datetime(year=2017, month=1, day=1)

    results = search(
        species=species,
        locations=locations,
        find_all=False,
        start_datetime=start,
        end_datetime=end,
    )

    results_list = sorted(list(results.keys()))

    expected = sorted(['ch4_bsd_108m_picarro', 'ch4_bsd_248m_picarro', 
                        'ch4_hfd_100m_picarro', 'ch4_tac_100m_picarro', 
                        'co2_bsd_108m_picarro', 'co2_bsd_248m_picarro', 
                        'co2_hfd_100m_picarro', 'co2_tac_100m_picarro'])

    assert results_list == expected

    assert len(results["co2_bsd_108m_picarro"]["keys"]['2014-01-30-13:33:30_2019-07-04-04:23:30']) == 23
    assert len(results["co2_hfd_100m_picarro"]["keys"]['2013-11-20-20:02:30_2019-07-04-21:29:30']) == 25
    assert len(results["co2_tac_100m_picarro"]["keys"]['2012-07-26-12:01:30_2019-07-04-09:58:30']) == 30
    assert len(results["ch4_bsd_108m_picarro"]["keys"]['2014-01-30-13:33:30_2019-07-04-04:23:30']) == 23
    assert len(results["ch4_hfd_100m_picarro"]["keys"]['2013-11-20-20:02:30_2019-07-04-21:29:30']) == 25
    assert len(results["ch4_tac_100m_picarro"]["keys"]['2012-07-26-12:01:30_2019-07-04-09:58:30']) == 30


def test_search_datetimes():
    species = ["co2"]
    locations = ["bsd"]

    start = get_datetime(year=2016, month=1, day=1)
    end = get_datetime(year=2017, month=1, day=1)

    results = search(
        species=species,
        locations=locations,
        find_all=False,
        start_datetime=start,
        end_datetime=end,
    )

    result_keys = results["co2_bsd_108m_picarro"]["keys"]

    date_strings = [v.split("/")[-1] for v in result_keys]

    assert date_strings == ['2016-01-19-17:17:30_2016-11-30-22:57:30']

    metadata = results["co2_bsd_108m_picarro"]["metadata"]

    expected_metadata = {
        "site": "bsd",
        "instrument": "picarro",
        "time_resolution": "1_minute",
        "inlet": "108m",
        "port": "9",
        "type": "air",
        "species": "co2",
        "data_type": "timeseries",
    }

    assert metadata == expected_metadata


def test_search_find_all():
    species = ["co2"]
    locations = ["bsd"]
    inlet = "108m"
    instrument = "picarro"

    start = get_datetime(year=2016, month=1, day=1)
    end = get_datetime(year=2017, month=1, day=1)

    results = search(
        species=species,
        locations=locations,
        find_all=True,
        start_datetime=start,
        end_datetime=end,
        inlet=inlet,
        instrument=instrument
    )

    bsd_results = results["co2_bsd_108m_picarro"]

    assert bsd_results["metadata"]["site"] == "bsd"
    assert bsd_results["metadata"]["species"] == "co2"
    assert bsd_results["metadata"]["time_resolution"] == "1_minute"

    key_dates = [daterange.split("/")[-1] for daterange in bsd_results["keys"]]

    assert key_dates == ['2016-01-19-17:17:30_2016-11-30-22:57:30']


def test_search_no_species(crds_read):
    locations = "bsd"

    results = search(locations=locations)

    expected_keys = sorted(['ch4_bsd_248m_picarro', 'co2_bsd_248m_picarro', 
                            'co_bsd_248m_picarro', 'co_bsd_108m_picarro5310', 
                            'n2o_bsd_108m_picarro5310', 'ch4_bsd_108m_picarro', 
                            'co2_bsd_108m_picarro', 'co_bsd_108m_picarro', 
                            'co_bsd_248m_picarro5310', 'n2o_bsd_248m_picarro5310'])

    assert sorted(list(results.keys())) == expected_keys


def test_search_with_inlet_instrument(crds_read):
    locations = "hfd"
    inlet = "100m"
    instrument = "picarro"
    species = "CH4"

    results = search(locations=locations, species=species, inlet=inlet, instrument=instrument)

    assert len(results["ch4_hfd_100m_picarro"]["keys"]["2013-11-20-20:02:30_2019-07-04-21:29:30"]) == 25

    expected_metadata = {'site': 'hfd', 'instrument': 'picarro', 'time_resolution': '1_minute', 
                        'inlet': '100m', 'port': '10', 'type': 'air', 'species': 'ch4', 'data_type': 'timeseries'}

    assert results["ch4_hfd_100m_picarro"]["metadata"] == expected_metadata


def test_search_inlet_no_instrument(crds_read):
    locations = "hfd"
    inlet = "100m"
    species = "CH4"

    results = search(locations=locations, species=species, inlet=inlet)

    expected_keys = ["ch4_hfd_100m_picarro"]

    assert list(results.keys()) == expected_keys

    assert len(results["ch4_hfd_100m_picarro"]["keys"]["2013-11-20-20:02:30_2019-07-04-21:29:30"]) == 25

    expected_metadata = {'site': 'hfd', 'instrument': 'picarro', 'time_resolution': '1_minute', 'inlet': '100m', 
                        'port': '10', 'type': 'air', 'species': 'ch4', 'data_type': 'timeseries'}

    assert results["ch4_hfd_100m_picarro"]["metadata"] == expected_metadata


def test_search_instrument_no_inlet(crds_read):
    locations = "bsd"
    species = "n2o"
    instrument = "picarro5310"

    results = search(locations=locations, species=species, instrument=instrument)

    expected_keys = ["n2o_bsd_108m_picarro5310", "n2o_bsd_248m_picarro5310"]

    assert sorted(list(results.keys())) == sorted(expected_keys)

    assert len(results["n2o_bsd_108m_picarro5310"]["keys"]["2019-03-06-14:03:30_2020-07-04-11:44:30"]) == 7
    assert len(results["n2o_bsd_248m_picarro5310"]["keys"]["2019-03-06-13:23:30_2020-07-05-03:38:30"]) == 7

    metadata_108m = {'site': 'bsd', 'instrument': 'picarro5310', 'time_resolution': '1_minute', 
                    'inlet': '108m', 'port': '2', 'type': 'air', 'species': 'n2o', 'data_type': 'timeseries'}

    metadata_248m = {'site': 'bsd', 'instrument': 'picarro5310', 'time_resolution': '1_minute', 
                    'inlet': '248m', 'port': '1', 'type': 'air', 'species': 'n2o', 'data_type': 'timeseries'}

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
