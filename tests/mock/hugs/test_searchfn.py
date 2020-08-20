import os
import pytest
from datetime import datetime
from pandas import Timestamp
from Acquire.Client import Service

from HUGS.Client import Process, RankSources, Search


@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("tmp_searchfn")
    return str(d)


def get_test_path_services(filename, data_type):
    """ Gets the path of the filename for a given data type

        Args:
            filename (str): Name of file, not path
            data_type (str): Data type, CRDS, GC, ICOS etc
        Returns:
            pathlib.Path: Absolute path to object

    """
    from pathlib import Path

    data_path = Path(__file__).resolve().parent.parent.parent.joinpath(f"data/proc_test_data/{data_type}/{filename}")

    return data_path




# @pytest.fixture(scope="session")
# def load_two_data(authenticated_user):
#     hugs = Service(service_url="hugs")
#     _ = hugs.call_function(function="clear_datasources", args={})

#     def test_folder(filename):
#         dir_path = os.path.dirname(__file__)
#         test_folder = "../../../tests/data/search_data"
#         return os.path.join(dir_path, test_folder, filename)

#     files = [
#         "bsd.picarro.1minute.108m.min.dat",
#         "hfd.picarro.1minute.100m.min.dat",
#         "tac.picarro.1minute.100m.min.dat",
#     ]
#     filepaths = [test_folder(f) for f in files]

#     process = Process(service_url="hugs")

#     process.process_files(
#         user=authenticated_user,
#         files=filepaths,
#         data_type="CRDS",
#         hugs_url="hugs",
#         storage_url="storage",
#     )

@pytest.fixture(scope="session")
def load_two_data(authenticated_user):
    hugs = Service(service_url="hugs")
    _ = hugs.call_function(function="clear_datasources", args={})

    def test_folder(filename):
        dir_path = os.path.dirname(__file__)
        test_folder = "../../../tests/data/search_data"
        return os.path.join(dir_path, test_folder, filename)

    crds_files = [
        "bsd.picarro5310.1minute.108m.min.dat",
        "bsd.picarro5310.1minute.248m.min.dat",
        "hfd.picarro.1minute.100m.min.dat",
    ]

    filepaths = [test_folder(filename=f) for f in crds_files]

    process = Process(service_url="hugs")

    process.process_files(
        user=authenticated_user,
        files=filepaths,
        data_type="CRDS",
        hugs_url="hugs",
        storage_url="storage",
    )

    dir_path = os.path.dirname(__file__)
    test_data = "../../../tests/data/proc_test_data/GC"
    data = os.path.join(dir_path, test_data, "capegrim-medusa.18.C")
    precision = os.path.join(dir_path, test_data, "capegrim-medusa.18.precisions.C")

    gc_files = [data, precision]
    # gc_files = [test_folder(f) for f in gc_files]

    process.process_files(
        user=authenticated_user,
        files=gc_files,
        data_type="CRDS",
        hugs_url="hugs",
        storage_url="storage",
    )


def test_search_hfd(load_two_data):
    search = Search(service_url="hugs")

    search_term = "co"
    location = "hfd"
    data_type = "CRDS"

    results = search.search(
        species=search_term, locations=location, data_type=data_type
    )

    hfd_res = results["co_hfd_100m"]

    assert len(hfd_res["keys"]["2013-11-20-20:02:30_2019-07-04-21:29:30"]) == 25

    expected_metadata = {
        "site": "hfd",
        "instrument": "picarro",
        "time_resolution": "1_minute",
        "inlet": "100m",
        "port": "10",
        "type": "air",
        "species": "co",
        "data_type": "timeseries",
    }

    assert hfd_res["metadata"] == expected_metadata


def test_search_and_rank_gc(load_two_data):
    r = RankSources(service_url="hugs")
    sources = r.get_sources(site="capegrim", species="NF3", data_type="GC")

    print(sources)

    search = Search(service_url="hugs")

    species = "SIO12"
    location = "CGO"
    data_type = "GC"

    results = search.search(
        species=species,
        locations=location,
        data_type=data_type,
        )

    print(results)


def test_search_and_rank(load_two_data):
    # First we need to rank the data
    r = RankSources(service_url="hugs")
    sources = r.get_sources(site="bsd", species="co", data_type="CRDS")

    uuid_108m = sources["co_bsd_108m_picarro5310"]["uuid"]
    uuid_248m = sources["co_bsd_248m_picarro5310"]["uuid"]

    del sources["co_bsd_108m_picarro5310"]["uuid"]
    del sources["co_bsd_248m_picarro5310"]["uuid"]

    assert sources == {
        "co_bsd_108m_picarro5310": {
            "rank": 0,
            "data_range": "2019-03-06T14:03:30_2020-07-04T11:44:30",
        },
        "co_bsd_248m_picarro5310": {
            "rank": 0,
            "data_range": "2019-03-06T13:23:30_2020-07-05T03:38:30",
        },
    }

    daterange_108 = r.create_daterange(
        start=datetime(2019, 3, 7), end=datetime(2019, 9, 15)
    )
    daterange_248 = r.create_daterange(
        start=datetime(2019, 9, 16), end=datetime(2020, 7, 5)
    )

    new_rankings = {
        "co_bsd_108m_picarro5310": {"rank": {1: [daterange_108]}, "uuid": uuid_108m},
        "co_bsd_248m_picarro5310": {"rank": {1: [daterange_248]}, "uuid": uuid_248m},
    }

    r.rank_sources(updated_rankings=new_rankings, data_type="CRDS")

    updated_sources = r.get_sources(site="bsd", species="co", data_type="CRDS")

    assert updated_sources["co_bsd_108m_picarro5310"]["rank"] == {
        "1": ["2019-03-07T00:00:00_2019-09-15T00:00:00"]
    }
    assert updated_sources["co_bsd_248m_picarro5310"]["rank"] == {
        "1": ["2019-09-16T00:00:00_2020-07-05T00:00:00"]
    }

    daterange_108_1 = r.create_daterange(
        start=datetime(2019, 3, 7), end=datetime(2019, 9, 15)
    )
    daterange_108_2 = r.create_daterange(
        start=datetime(2019, 11, 6), end=datetime(2020, 7, 5)
    )
    daterange_248 = r.create_daterange(
        start=datetime(2019, 9, 16), end=datetime(2019, 11, 5)
    )
    # Change in ranking
    new_rankings = {
        "co_bsd_108m_picarro5310": {
            "rank": {1 : [daterange_108_1, daterange_108_2]},
            "uuid": uuid_108m,
        },
        "co_bsd_248m_picarro5310": {"rank": {1: [daterange_248]}, "uuid": uuid_248m},
    }

    r.rank_sources(updated_rankings=new_rankings, data_type="CRDS")

    updated_sources = r.get_sources(site="bsd", species="co", data_type="CRDS")

    assert updated_sources["co_bsd_108m_picarro5310"]["rank"] == {'1': ['2019-03-07-00:00:00+00:00_2019-09-15-00:00:00+00:00', 
                                                                        '2019-11-06-00:00:00+00:00_2020-07-05-00:00:00+00:00']}
    assert updated_sources["co_bsd_248m_picarro5310"]["rank"] == {'1': ['2019-09-16-00:00:00+00:00_2020-07-05-00:00:00+00:00']}

    # Now we need to search for the data and ensure we get the correct data keys returned
    search = Search(service_url="hugs")

    species = "co"
    location = "bsd"
    data_type = "CRDS"

    start_search = Timestamp(2019, 3, 7)
    end_search = Timestamp(2020, 10, 30)

    results = search.search(
        species=species,
        locations=location,
        data_type=data_type,
        start_datetime=start_search,
        end_datetime=end_search,
    )

    assert results["co_bsd_108m_picarro5310"]["metadata"] == {
        "site": "bsd",
        "instrument": "picarro5310",
        "time_resolution": "1_minute",
        "inlet": "108m",
        "port": "2",
        "type": "air",
        "species": "co",
        "data_type": "timeseries",
    }

    assert (
        len(results["co_bsd_108m_picarro5310"]["keys"]["2019-03-07-00:00:00_2019-09-15-00:00:00"])
        == 3
    )
    assert (
        len(results["co_bsd_108m_picarro5310"]["keys"]["2019-11-06-00:00:00_2020-07-05-00:00:00"])
        == 5
    )
    assert (
        len(results["co_bsd_248m_picarro5310"]["keys"]["2019-09-16-00:00:00_2020-07-05-00:00:00"])
        == 5
    )

    assert results["co_bsd_248m_picarro5310"]["metadata"] == {
        "site": "bsd",
        "instrument": "picarro5310",
        "time_resolution": "1_minute",
        "inlet": "248m",
        "port": "1",
        "type": "air",
        "species": "co",
        "data_type": "timeseries",
    }


def test_single_site_search(load_two_data):
    search = Search(service_url="hugs")

    species = "co"
    location = "hfd"
    data_type = "CRDS"
    inlet = "100m"
    instrument = "picarro"

    results = search.search(
        species=species,
        locations=location,
        data_type=data_type,
        inlet=inlet,
        instrument=instrument,
    )

    assert (
        len(
            results["co_hfd_picarro_100m"]["keys"][
                "2013-11-20-20:02:30_2019-07-04-21:29:30"
            ]
        )
        == 25
    )

    assert results["co_hfd_picarro_100m"]["metadata"] == {
        "site": "hfd",
        "instrument": "picarro",
        "time_resolution": "1_minute",
        "inlet": "100m",
        "port": "10",
        "type": "air",
        "species": "co",
        "data_type": "timeseries",
    }


# def test_search_multispecies_singlesite(load_two_data):
#     search = Search(service_url="hugs")

#     search_term = ["co", "co2"]
#     location = "bsd"
#     data_type = "CRDS"

#     results = search.search(species=search_term, locations=location, data_type=data_type)

#     assert sorted(list(results.keys())) == sorted(["co_bsd_108m", "co_bsd_248m"])
#     assert "2019-03-07-00:00:00_2019-09-15-00:00:00" in results["co_bsd_108m"]["keys"]
#     assert "2019-09-16-00:00:00_2019-11-05-00:00:00" in results["co_bsd_248m"]["keys"]


# def test_search_multisite_co(load_two_data):
#     search = Search(service_url="hugs")

#     search_term = "co"
#     location = ["bsd", "hfd", "tac"]
#     data_type = "CRDS"

#     results = search.search(
#         species=search_term, locations=location, data_type=data_type
#     )

#     assert sorted(list(results.keys())) == sorted(
#         ["co_bsd_108m", "co_hfd_100m", "co_bsd_248m"]
#     )

#     assert "2013-11-20-20:02:30_2019-07-04-21:29:30" in results["co_hfd_100m"]["keys"]


# def test_search_multiplesite_multiplespecies(load_two_data):
#     search = Search(service_url="hugs")

#     search_term = ["ch4", "co2"]
#     location = ["bsd", "hfd", "tac"]
#     data_type = "CRDS"

#     results = search.search(
#         species=search_term, locations=location, data_type=data_type
#     )

#     expected_keys = [
#         "ch4_bsd_108m",
#         "co2_bsd_108m",
#         "ch4_hfd_100m",
#         "co2_hfd_100m",
#         "ch4_tac_100m",
#         "co2_tac_100m",
#     ]

#     assert sorted(list(results.keys())) == sorted(expected_keys)

#     expected_keys = [
#         "2014-01-30-13:33:30_2019-07-04-04:23:30",
#         "2014-01-30-13:33:30_2019-07-04-04:23:30",
#         "2013-11-20-20:02:30_2019-07-04-21:29:30",
#         "2013-11-20-20:02:30_2019-07-04-21:29:30",
#         "2012-07-26-12:01:30_2019-07-04-09:58:30",
#         "2012-07-26-12:01:30_2019-07-04-09:58:30",
#     ]

#     keys = []
#     for key in results:
#         keys.append(list(results[key]["keys"].keys())[0])

#     assert sorted(keys) == sorted(expected_keys)


# def test_returns_readable_results():
#     search = Search(service_url="hugs")

#     search_term = ["ch4"]
#     location = ["bsd"]

#     search.search(species=search_term, locations=location, data_type="CRDS")

#     assert search.results() == {'ch4_bsd_108m': 'Daterange : 2014-01-30-13:33:30+00:00 - 2019-07-04-04:23:30+00:00'}


# def test_search_download(load_two_data):
#     search = Search(service_url="hugs")

#     search_term = ["ch4"]
#     location = ["bsd"]

#     search.search(species=search_term, locations=location, data_type="CRDS")

#     data = search.download("ch4_bsd_108m")

#     data_attributes = data["ch4_bsd_108m"].attrs
#     assert data_attributes["data_owner"] == "Simon O'Doherty"
#     assert data_attributes["station_longitude"] == pytest.approx(-1.15033)
#     assert data_attributes["station_latitude"] == pytest.approx(54.35858)
#     assert data_attributes["station_long_name"] == "Bilsdale, UK"
#     assert data["ch4_bsd_108m"]["ch4"][0] == pytest.approx(1960.25)
