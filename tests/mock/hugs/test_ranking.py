import pytest
import os

from HUGS.Client import RankSources, Process
from HUGS.ObjectStore import get_local_bucket

@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("tmp_searchfn")
    return str(d)


@pytest.fixture(scope="session")
def load_crds(authenticated_user):
    get_local_bucket(empty=True)

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


@pytest.mark.skip(reason="Need to fix dependence on Acquire")
def test_set_ranking(authenticated_user, load_crds):
    r = RankSources(service_url="hugs")

    sources = r.get_sources(site="hfd", species="ch4")

    fifty_metre_uuid = sources["ch4_hfd_50m_picarro"]["uuid"]
    hundred_metre_uuid = sources["ch4_hfd_100m_picarro"]["uuid"]

    del sources["ch4_hfd_100m_picarro"]["uuid"]
    del sources["ch4_hfd_50m_picarro"]["uuid"]

    expected_sources = {'ch4_hfd_100m_picarro': {'rank': 0, 'data_range': '2013-12-04T14:02:30_2019-05-21T15:46:30'}, 
                        'ch4_hfd_50m_picarro': {'rank': 0, 'data_range': '2013-11-23T12:28:30_2020-06-24T09:41:30'}}

    assert sources == expected_sources

    new_rankings = {'ch4_hfd_100m_picarro': {'rank': {1: ["2013-12-04T14:02:30_2019-05-21T15:46:30"]}, 'uuid': hundred_metre_uuid}, 
                    'ch4_hfd_50m_picarro': {'rank': {2: ['2013-11-23T12:28:30_2020-06-24T09:41:30']}, 'uuid': fifty_metre_uuid}}

    r.rank_sources(updated_rankings=new_rankings)

    sources = r.get_sources(site="hfd", species="ch4")

    del sources["ch4_hfd_100m_picarro"]["uuid"]
    del sources["ch4_hfd_50m_picarro"]["uuid"]

    expected = {'ch4_hfd_100m_picarro': {'rank': {'1': ['2013-12-04T14:02:30_2019-05-21T15:46:30']}, 
                'data_range': '2013-12-04T14:02:30_2019-05-21T15:46:30'}, 
                'ch4_hfd_50m_picarro': {'rank': {'2': ['2013-11-23T12:28:30_2020-06-24T09:41:30']}, 
                'data_range': '2013-11-23T12:28:30_2020-06-24T09:41:30'}}

    assert sources == expected
