import os
import pytest
from openghg.localclient import RankSources
from openghg.modules import ObsSurface
from openghg.objectstore import get_local_bucket


# Ensure we have something to rank
@pytest.fixture(scope="session", autouse=True)
def crds():
    get_local_bucket(empty=True)
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filename = "hfd.picarro.1minute.100m.min.dat"
    filepath = os.path.join(dir_path, test_data, filename)

    ObsSurface.read_file(filepath=filepath, data_type="CRDS", site="hfd", network="DECC")

@pytest.mark.skip(reason="RankSources class needs updating")
def test_ranking(crds):
    r = RankSources()

    results = r.get_sources(site="hfd", species="co2")

    hundred_uuid = results["co2_hfd_100m_picarro"]["uuid"]

    del results["co2_hfd_100m_picarro"]["uuid"]

    expected_results = {"co2_hfd_100m_picarro": {"rank": 0, "data_range": "2013-12-04T14:02:30_2019-05-21T15:46:30"}}

    expected_results = {
        "co2_hfd_100m_picarro": {
            "rank": 0,
            "data_range": "2013-12-04T14:02:30_2019-05-21T15:46:30",
            "metadata": {
                "site": "hfd",
                "instrument": "picarro",
                "time_resolution": "1_minute",
                "inlet": "100m",
                "port": "10",
                "type": "air",
                "species": "co2",
                "scale": "wmo-x2007",
                "data_type": "timeseries",
            },
        }
    }

    assert results == expected_results

    rank_daterange = r.create_daterange(start="2013-12-04", end="2016-05-05")

    updated = {
        "co2_hfd_100m_picarro": {
            "rank": {1: [rank_daterange]},
            "data_range": "2013-12-04T14:02:30_2019-05-21T15:46:30",
            "uuid": hundred_uuid,
        }
    }

    r.rank_sources(updated_rankings=updated)

    results = r.get_sources(site="hfd", species="co2")

    del results["co2_hfd_100m_picarro"]["uuid"]

    expected_results = {
        "co2_hfd_100m_picarro": {
            "rank": {"1": ["2013-12-04T00:00:00_2016-05-05T00:00:00"]},
            "data_range": "2013-12-04T14:02:30_2019-05-21T15:46:30",
            "metadata": {
                "site": "hfd",
                "instrument": "picarro",
                "time_resolution": "1_minute",
                "inlet": "100m",
                "port": "10",
                "type": "air",
                "species": "co2",
                "scale": "wmo-x2007",
                "data_type": "timeseries",
            },
        }
    }

    assert results == expected_results
