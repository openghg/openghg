import os
import pytest

from openghg.modules import ObsSurface
from openghg.objectstore import get_local_bucket
from openghg.localclient import Search

# Ensure we have something to find
@pytest.fixture(scope="session", autouse=True)
def crds():
    get_local_bucket(empty=True)
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filename = "hfd.picarro.1minute.100m.min.dat"
    filepath = os.path.join(dir_path, test_data, filename)

    ObsSurface.read_file(filepath=filepath, data_type="CRDS")


def test_search_and_download(crds):
    s = Search()

    results = s.search(species="co2", locations="hfd")

    assert len(results["co2_hfd_100m_picarro"]["keys"]["2013-12-04-14:02:30_2019-05-21-15:46:30"]) == 23

    expected_metadata = {'site': 'hfd', 'instrument': 'picarro', 'time_resolution': '1_minute', 
                        'inlet': '100m', 'port': '10', 'type': 'air', 'species': 'co2', 
                        'data_type': 'timeseries', 'scale': 'wmo-x2007'}

    assert results["co2_hfd_100m_picarro"]["metadata"] == expected_metadata

    data = s.retrieve(selected_keys="co2_hfd_100m_picarro")

    assert data["co2_hfd_100m_picarro"]["2013-12-04-14:02:30_2019-05-21-15:46:30"]["co2"][0] == pytest.approx(414.21)

    assert data["co2_hfd_100m_picarro"]["2013-12-04-14:02:30_2019-05-21-15:46:30"]["co2_stdev"][-1] == pytest.approx(0.247)
    assert data["co2_hfd_100m_picarro"]["2013-12-04-14:02:30_2019-05-21-15:46:30"]["co2_n_meas"][10] == 19.0
