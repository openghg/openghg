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

    ObsSurface.read_file(filepath=(data_filepath, prec_filepath), data_type="GCWERKS")


@pytest.fixture(scope="session")
def crds_read():
    get_local_bucket(empty=True)
    test_data = "../data/search_data"
    folder_path = os.path.join(os.path.dirname(__file__), test_data)
    ObsSurface.read_folder(folder_path=folder_path, data_type="CRDS", extension="dat")


def test_search_gc(gc_read):
    results = search(species=["NF3"], locations="capegrim")

    nf3_results = results["nf3_cgo_75m_4"]

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

    expected = sorted(
        [
            "ch4_bsd_248m",
            "ch4_bsd_108m",
            "co2_bsd_248m",
            "co2_bsd_108m",
            "ch4_hfd_100m",
            "co2_hfd_100m",
            "ch4_tac_100m",
            "co2_tac_100m",
        ]
    )

    assert results_list == expected

    assert len(results["co2_bsd_108m"]["keys"]['2014-01-30-13:33:30_2019-07-04-04:23:30']) == 23
    assert len(results["co2_hfd_100m"]["keys"]['2013-11-20-20:02:30_2019-07-04-21:29:30']) == 25
    assert len(results["co2_tac_100m"]["keys"]['2012-07-26-12:01:30_2019-07-04-09:58:30']) == 30
    assert len(results["ch4_bsd_108m"]["keys"]['2014-01-30-13:33:30_2019-07-04-04:23:30']) == 23
    assert len(results["ch4_hfd_100m"]["keys"]['2013-11-20-20:02:30_2019-07-04-21:29:30']) == 25
    assert len(results["ch4_tac_100m"]["keys"]['2012-07-26-12:01:30_2019-07-04-09:58:30']) == 30


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

    result_keys = results["co2_bsd_108m"]["keys"]

    date_strings = [v.split("/")[-1] for v in result_keys]

    assert date_strings == ['2016-01-19-17:17:30_2016-11-30-22:57:30']

    metadata = results["co2_bsd_108m"]["metadata"]

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

    bsd_results = results["co2_bsd_picarro_108m"]

    assert bsd_results["metadata"]["site"] == "bsd"
    assert bsd_results["metadata"]["species"] == "co2"
    assert bsd_results["metadata"]["time_resolution"] == "1_minute"

    key_dates = [daterange.split("/")[-1] for daterange in bsd_results["keys"]]

    assert key_dates == ['2016-01-19-17:17:30_2016-11-30-22:57:30']

    # assert key_dates == sorted(["2016-01-19-17:17:30+00:00_2016-12-31-23:52:30+00:00",
    #                             "2016-06-01-00:23:30+00:00_2016-08-31-23:58:30+00:00",
    #                             "2016-03-01-02:22:30+00:00_2016-05-31-22:15:30+00:00",
    #                             "2016-09-01-02:48:30+00:00_2016-11-30-22:57:30+00:00"])


def test_search_no_species(crds_read):
    locations = "bsd"

    results = search(locations=locations)

    expected_keys = sorted(['ch4_bsd_picarro_248m', 'co2_bsd_picarro_248m', 'co_bsd_picarro_248m', 
                            'co_bsd_picarro5310_108m', 'n2o_bsd_picarro5310_108m', 'ch4_bsd_picarro_108m', 
                            'co2_bsd_picarro_108m', 'co_bsd_picarro_108m', 'co_bsd_picarro5310_248m', 
                            'n2o_bsd_picarro5310_248m'])

    assert sorted(list(results.keys())) == expected_keys


def test_search_with_inlet_instrument(crds_read):
    locations = "hfd"
    inlet = "100m"
    instrument = "picarro"
    species = "CH4"

    results = search(locations=locations, species=species, inlet=inlet, instrument=instrument)

    assert len(results["ch4_hfd_picarro_100m"]["keys"]["2013-11-20-20:02:30_2019-07-04-21:29:30"]) == 25

    expected_metadata = {'site': 'hfd', 'instrument': 'picarro', 'time_resolution': '1_minute', 
                        'inlet': '100m', 'port': '10', 'type': 'air', 'species': 'ch4', 'data_type': 'timeseries'}

    assert results["ch4_hfd_picarro_100m"]["metadata"] == expected_metadata


def test_search_inlet_no_instrument(crds_read):
    locations = "hfd"
    inlet = "100m"
    species = "CH4"

    results = search(locations=locations, species=species, inlet=inlet)

    expected_keys = ["ch4_hfd_100m"]

    assert list(results.keys()) == expected_keys

    assert len(results["ch4_hfd_100m"]["keys"]["2013-11-20-20:02:30_2019-07-04-21:29:30"]) == 25

    expected_metadata = {'site': 'hfd', 'instrument': 'picarro', 'time_resolution': '1_minute', 'inlet': '100m', 
                        'port': '10', 'type': 'air', 'species': 'ch4', 'data_type': 'timeseries'}

    assert results["ch4_hfd_100m"]["metadata"] == expected_metadata


def test_search_instrument_no_inlet(crds_read):
    locations = "bsd"
    species = "n2o"
    instrument = "picarro5310"

    results = search(locations=locations, species=species, instrument=instrument)

    expected_keys = ["n2o_bsd_108m", "n2o_bsd_248m"]

    assert sorted(list(results.keys())) == sorted(expected_keys)

    assert len(results["n2o_bsd_108m"]["keys"]["2019-03-06-14:03:30_2020-07-04-11:44:30"]) == 7
    assert len(results["n2o_bsd_248m"]["keys"]["2019-03-06-13:23:30_2020-07-05-03:38:30"]) == 7

    metadata_108m = {'site': 'bsd', 'instrument': 'picarro5310', 'time_resolution': '1_minute', 
                    'inlet': '108m', 'port': '2', 'type': 'air', 'species': 'n2o', 'data_type': 'timeseries'}

    metadata_248m = {'site': 'bsd', 'instrument': 'picarro5310', 'time_resolution': '1_minute', 
                    'inlet': '248m', 'port': '1', 'type': 'air', 'species': 'n2o', 'data_type': 'timeseries'}

    assert results["n2o_bsd_108m"]["metadata"] == metadata_108m
    assert results["n2o_bsd_248m"]["metadata"] == metadata_248m


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
