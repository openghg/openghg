import os
import pytest

from HUGS.Modules import CRDS
from HUGS.ObjectStore import get_local_bucket
from HUGS.LocalClient import Search

# Ensure we have something to find
@pytest.fixture(scope="session", autouse=True)
def crds():
    get_local_bucket(empty=True)
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filename = "hfd.picarro.1minute.100m.min.dat"
    filepath = os.path.join(dir_path, test_data, filename)

    CRDS.read_file(data_filepath=filepath, source_name="hfd_picarro_100m", site="hfd")


def test_search_and_download(crds):
    s = Search()

    results = s.search(species="co2", locations="hfd", data_type="CRDS")

    assert len(results["co2_hfd_100m"]["keys"]["2013-12-04-14:02:30_2019-05-21-15:46:30"]) == 23

    expected_metadata = {'site': 'hfd', 'instrument': 'picarro', 'time_resolution': '1_minute', 
                        'inlet': '100m', 'port': '10', 'type': 'air', 'species': 'co2', 
                        'data_type': 'timeseries'}

    assert results["co2_hfd_100m"]["metadata"] == expected_metadata

    data = s.retrieve(selected_keys="co2_hfd_100m")

    assert data["co2_hfd_100m"]["2013-12-04-14:02:30_2019-05-21-15:46:30"]["co2"][0] == pytest.approx(414.21)

    assert data["co2_hfd_100m"]["2013-12-04-14:02:30_2019-05-21-15:46:30"]["co2_stdev"][-1] == pytest.approx(0.247)
    assert data["co2_hfd_100m"]["2013-12-04-14:02:30_2019-05-21-15:46:30"]["co2_n_meas"][10] == 19.0


