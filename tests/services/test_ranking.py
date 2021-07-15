import pytest

from openghg.client import RankSources, Process
from openghg.objectstore import get_local_bucket
from helpers import get_datapath

@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("tmp_searchfn")
    return str(d)


@pytest.fixture(scope="session")
def load_crds(authenticated_user):
    get_local_bucket(empty=True)

    files = [
        "hfd.picarro.1minute.100m.min.dat",
        "hfd.picarro.1minute.50m.min.dat",
    ]
    filepaths = [get_datapath(filename=f, data_type="CRDS") for f in files]

    process = Process(service_url="openghg")

    process.process_files(
        user=authenticated_user,
        files=filepaths,
        site="hfd",
        network="DECC",
        data_type="CRDS",
        openghg_url="openghg",
        storage_url="storage",
    )


def test_set_and_clear_ranking(authenticated_user, load_crds):
    r = RankSources(service_url="openghg")

    response = r.get_sources(site="hfd", species="co2")

    expected_response = {
        "co2_100m_picarro": {"rank_data": "NA", "data_range": "2013-12-04T14:02:30_2019-05-21T15:46:30"},
        "co2_50m_picarro": {"rank_data": "NA", "data_range": "2013-11-23T12:28:30_2020-06-24T09:41:30"},
    }

    assert response == expected_response

    r.set_rank(key="co2_100m_picarro", rank=1, start_date="2015-01-01", end_date="2017-01-01")

    response = r.get_sources(site="hfd", species="co2")

    expected_new_response = {
        "co2_100m_picarro": {
            "rank_data": {"2015-01-01-00:00:00+00:00_2017-01-01-00:00:00+00:00": 1},
            "data_range": "2013-12-04T14:02:30_2019-05-21T15:46:30",
        },
        "co2_50m_picarro": {"rank_data": "NA", "data_range": "2013-11-23T12:28:30_2020-06-24T09:41:30"},
    }

    assert response == expected_new_response

    r.clear_rank(key="co2_100m_picarro")

    response = r.get_sources(site="hfd", species="co2")

    expected_clear_response = {
        "co2_100m_picarro": {"rank_data": "NA", "data_range": "2013-12-04T14:02:30_2019-05-21T15:46:30"},
        "co2_50m_picarro": {"rank_data": "NA", "data_range": "2013-11-23T12:28:30_2020-06-24T09:41:30"},
    }

    assert response == expected_clear_response
