import pytest
from openghg.localclient import RankSources
from openghg.store import ObsSurface
from openghg.objectstore import get_local_bucket
from openghg.util import bilsdale_datapaths


# Ensure we have something to rank
@pytest.fixture(scope="session", autouse=True)
def crds():
    get_local_bucket(empty=True)
    bsd_paths = bilsdale_datapaths()
    ObsSurface.read_file(filepath=bsd_paths, data_type="CRDS", site="bsd", network="DECC")


def test_set_rank():
    r = RankSources()

    results = r.get_sources(site="bsd", species="co2")

    expected_results = {
        "co2_248m_picarro": {"rank_data": "NA", "data_range": "2014-01-30T11:12:30_2020-12-01T22:31:30"},
        "co2_42m_picarro": {"rank_data": "NA", "data_range": "2014-01-30T11:12:30_2020-12-01T22:31:30"},
        "co2_108m_picarro": {"rank_data": "NA", "data_range": "2014-01-30T11:12:30_2020-12-01T22:31:30"},
    }

    assert results == expected_results

    r.set_rank(key="co2_42m_picarro", rank=1, start_date="2001-01-01", end_date="2007-01-01")

    specific_results = r.get_specific_source(key="co2_42m_picarro")

    assert specific_results == {"2001-01-01-00:00:00+00:00_2007-01-01-00:00:00+00:00": 1}


def test_clear_rank():
    r = RankSources()

    results = r.get_sources(site="bsd", species="co2")
    r.set_rank(key="co2_42m_picarro", rank=1, start_date="2001-01-01", end_date="2007-01-01")

    r.clear_rank(key="co2_42m_picarro")

    specific_result = r.get_specific_source(key="co2_42m_picarro")

    assert specific_result == "NA"

    results = r.get_sources(site="bsd", species="co2")

    assert results["co2_42m_picarro"]["rank_data"] == "NA"

    with pytest.raises(ValueError):
        r.clear_rank(key="co2_42m_picarro")
