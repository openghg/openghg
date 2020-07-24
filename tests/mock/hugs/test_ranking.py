import pytest
import os

from HUGS.Client import RankSources, Process
from Acquire.Client import Service


@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("tmp_searchfn")
    return str(d)


@pytest.fixture(scope="session")
def load_crds(authenticated_user):
    hugs = Service(service_url="hugs")
    _ = hugs.call_function(function="clear_datasources", args={})

    def test_folder(filename):
        dir_path = os.path.dirname(__file__)
        test_folder = "../../../tests/data/proc_test_data/CRDS"
        return os.path.join(dir_path, test_folder, filename)

    files = [
        "hfd.picarro.1minute.100m.min.dat",
        "hfd.picarro.1minute.50m.min.dat",
    ]
    filepaths = [test_folder(f) for f in files]

    process = Process(service_url="hugs")

    process.process_files(
        user=authenticated_user,
        files=filepaths,
        data_type="CRDS",
        hugs_url="hugs",
        storage_url="storage",
    )


def test_get_sources(authenticated_user, load_crds):
    r = RankSources(service_url="hugs")
    sources = r.get_sources(site="hfd", species="co2", data_type="CRDS")

    # original_sources = copy.deepcopy(sources)

    del sources["hfd_co2_100m"]["uuid"]
    del sources["hfd_co2_50m"]["uuid"]

    expected_sources = {'hfd_co2_100m': {'rank': -1, 'daterange': '2013-12-04T14:02:30_2019-05-21T15:46:30'}, 
                        'hfd_co2_50m': {'rank': -1, 'daterange': '2013-11-23T12:28:30_2020-06-24T09:41:30'}}

    assert sources == expected_sources


def test_set_ranking(authenticated_user, load_crds):
    r = RankSources(service_url="hugs")

    sources = r.get_sources(site="hfd", species="ch4", data_type="CRDS")

    # original_sources = copy.deepcopy(sources)

    fifty_metre_uuid = sources["hfd_ch4_50m"]["uuid"]
    hundred_metre_uuid = sources["hfd_ch4_100m"]["uuid"]

    del sources["hfd_ch4_100m"]["uuid"]
    del sources["hfd_ch4_50m"]["uuid"]

    expected_sources = {'hfd_ch4_100m': {'rank': -1, 'daterange': '2013-12-04T14:02:30_2019-05-21T15:46:30'}, 
                        'hfd_ch4_50m': {'rank': -1, 'daterange': '2013-11-23T12:28:30_2020-06-24T09:41:30'}}

    assert sources == expected_sources

    new_rankings = {'hfd_ch4_100m': {'rank': 1, 'daterange': '2013-12-04T14:02:30_2019-05-21T15:46:30', 'uuid': hundred_metre_uuid}, 
                    'hfd_ch4_50m': {'rank': 2, 'daterange': '2013-11-23T12:28:30_2020-06-24T09:41:30', 'uuid': fifty_metre_uuid}}

    r.rank_sources(updated_rankings=new_rankings)

    sources = r.get_sources(site="hfd", species="ch4", data_type="CRDS")

    assert sources == new_rankings
