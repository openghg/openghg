import pytest
from pandas import Timestamp
from openghg.retrieve import search
from openghg.localclient import RankSources
from openghg.store import ObsSurface
from openghg.objectstore import get_local_bucket

from helpers import get_datapath


@pytest.fixture(scope="session", autouse=True)
def load_CRDS():
    get_local_bucket(empty=True)

    tac_100m = get_datapath("tac.picarro.1minute.100m.min.dat", data_type="CRDS")
    hfd_50m = get_datapath("hfd.picarro.1minute.50m.min.dat", data_type="CRDS")
    bsd_42m = get_datapath("bsd.picarro.1minute.42m.min.dat", data_type="CRDS")
    bsd_108m = get_datapath("bsd.picarro.1minute.108m.min.dat", data_type="CRDS")

    ObsSurface.read_file(filepath=tac_100m, data_type="CRDS", site="tac", network="DECC")
    ObsSurface.read_file(filepath=hfd_50m, data_type="CRDS", site="hfd", network="DECC")
    ObsSurface.read_file(filepath=bsd_42m, data_type="CRDS", site="bsd", network="DECC")
    ObsSurface.read_file(filepath=bsd_108m, data_type="CRDS", site="bsd", network="DECC")


def test_retrieve_unranked():
    results = search(species="ch4", skip_ranking=True)

    assert results.ranked_data is False
    assert results.cloud is False

    raw_results = results.raw()
    assert raw_results["tac"]["ch4"]["100m"]
    assert raw_results["hfd"]["ch4"]["50m"]
    assert raw_results["bsd"]["ch4"]["42m"]


def test_retrieve_complex_ranked():
    rank = RankSources()

    rank.get_sources(site="hfd", species="co2")
    rank.set_rank(key="co2_50m_picarro", rank=1, start_date="2015-01-01", end_date="2020-04-01")
    rank.get_sources(site="bsd", species="co")
    rank.set_rank(key="co_42m_picarro", rank=1, start_date="2013-01-01", end_date="2020-01-01")

    results = search(species="co2")

    assert results.ranked_data is True

    raw_results = results.raw()

    # Here we'll only get ranked data
    assert raw_results["hfd"]["co2"]
    assert "bsd" not in raw_results

    # Make sure we get the correct data
    data_inlet = results.retrieve(inlet="50m")["hfd_co2"]
    data_inlet = data_inlet.data

    assert data_inlet.time[0] == Timestamp("2013-11-23T12:28:30")
    assert data_inlet.co2[0] == 404.95

    data_species = results.retrieve(species="co2")["hfd_co2"]
    data_species = data_species.data

    assert data_species.equals(data_inlet)

    data_site = results.retrieve(site="hfd")["hfd_co2"]
    data_site = data_site.data

    assert data_site.equals(data_inlet)

    data_all = results.retrieve(site="hfd", inlet="50m", species="co2")
    data_all = data_all.data

    assert data_all.equals(data_inlet)

    results = search(species="ch4", skip_ranking=True)

    raw_results = results.raw()

    assert raw_results["tac"]["ch4"]["100m"]
    assert raw_results["hfd"]["ch4"]["50m"]
    assert raw_results["bsd"]["ch4"]["42m"]
    assert raw_results["bsd"]["ch4"]["108m"]
