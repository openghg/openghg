import pytest
from pandas import Timestamp
from openghg.retrieve import search
from openghg.client import rank_sources
from openghg.store import ObsSurface
from openghg.util import split_daterange_str
import numpy as np

from helpers import get_datapath


@pytest.fixture(scope="session", autouse=True)
def load_CRDS():
    tac_100m = get_datapath("tac.picarro.1minute.100m.min.dat", data_type="CRDS")
    hfd_50m = get_datapath("hfd.picarro.1minute.50m.min.dat", data_type="CRDS")
    bsd_42m = get_datapath("bsd.picarro.1minute.42m.min.dat", data_type="CRDS")
    bsd_108m = get_datapath("bsd.picarro.1minute.108m.min.dat", data_type="CRDS")
    bsd_248m = get_datapath("bsd.picarro.1minute.248m.min.dat", data_type="CRDS")

    ObsSurface.read_file(filepath=tac_100m, data_type="CRDS", site="tac", network="DECC")
    ObsSurface.read_file(filepath=hfd_50m, data_type="CRDS", site="hfd", network="DECC")
    ObsSurface.read_file(filepath=[bsd_42m, bsd_108m, bsd_248m], data_type="CRDS", site="bsd", network="DECC")


def test_retrieve_unranked():
    results = search(species="ch4", skip_ranking=True)

    assert results.ranked_data is False
    assert results.cloud is False

    raw_results = results.raw()
    assert raw_results["tac"]["ch4"]["100m"]
    assert raw_results["hfd"]["ch4"]["50m"]
    assert raw_results["bsd"]["ch4"]["42m"]


def test_retrieve_complex_ranked():
    # Clear the ObsSurface ranking data
    obs = ObsSurface.load()
    obs._rank_data.clear()
    obs.save()

    rank = rank_sources(site="bsd", species="co")

    expected_res = {
        "42m": {"rank_data": "NA", "data_range": "2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00"},
        "108m": {"rank_data": "NA", "data_range": "2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00"},
        "248m": {"rank_data": "NA", "data_range": "2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00"},
    }

    assert rank.raw() == expected_res

    rank.set_rank(inlet="42m", rank=1, start_date="2014-01-01", end_date="2015-03-01")
    rank.set_rank(inlet="108m", rank=1, start_date="2015-03-02", end_date="2016-08-01")
    rank.set_rank(inlet="42m", rank=1, start_date="2016-08-02", end_date="2017-03-01")
    rank.set_rank(inlet="248m", rank=1, start_date="2017-03-02", end_date="2019-03-01")
    rank.set_rank(inlet="108m", rank=1, start_date="2019-03-02", end_date="2021-12-01")

    updated_res = rank.get_sources(site="bsd", species="co")

    expected_updated_res = {
        "42m": {
            "rank_data": {
                "2014-01-01-00:00:00+00:00_2015-03-01-00:00:00+00:00": 1,
                "2016-08-02-00:00:00+00:00_2017-03-01-00:00:00+00:00": 1,
            },
            "data_range": "2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00",
        },
        "108m": {
            "rank_data": {
                "2015-03-02-00:00:00+00:00_2016-08-01-00:00:00+00:00": 1,
                "2019-03-02-00:00:00+00:00_2021-12-01-00:00:00+00:00": 1,
            },
            "data_range": "2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00",
        },
        "248m": {
            "rank_data": {"2017-03-02-00:00:00+00:00_2019-03-01-00:00:00+00:00": 1},
            "data_range": "2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00",
        },
    }

    assert updated_res == expected_updated_res

    search_res = search(site="bsd", species="co")

    obsdata = search_res.retrieve(site="bsd", species="co")

    metadata = obsdata.metadata

    expected_rank_metadata = {
        "ranked": {
            "2014-01-01-00:00:00+00:00_2015-03-01-00:00:00+00:00": "42m",
            "2016-08-02-00:00:00+00:00_2017-03-01-00:00:00+00:00": "42m",
            "2015-03-02-00:00:00+00:00_2016-08-01-00:00:00+00:00": "108m",
            "2019-03-02-00:00:00+00:00_2021-12-01-00:00:00+00:00": "108m",
            "2017-03-02-00:00:00+00:00_2019-03-01-00:00:00+00:00": "248m",
        },
        "unranked": {},
    }
    assert metadata["rank_metadata"] == expected_rank_metadata

    measurement_data = obsdata.data

    unique, count = np.unique(measurement_data.time, return_counts=True)
    # Ensure there are no duplicates
    assert unique[count > 1].size == 0

    # Make sure the inlets have been written to the Dataset correctly
    for daterange, inlet in expected_rank_metadata["ranked"].items():
        start, end = split_daterange_str(daterange, date_only=True)
        d = measurement_data.sel(time=slice(str(start), str(end)))

        all_inlets = np.unique(d["inlet"])

        assert all_inlets.size == 1
        assert all_inlets[0] == inlet

    assert measurement_data.time.size == 126
