import os
from HUGS.LocalClient import process_files
from HUGS.ObjectStore import get_local_bucket


def test_process_files():
    get_local_bucket(empty=True)
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filename = "hfd.picarro.1minute.100m.min.dat"
    filepath = os.path.join(dir_path, test_data, filename)

    results = process_files(files=filepath, site="hfd", instrument="picarro", network="DECC", data_type="CRDS")

    results = results["hfd.picarro.1minute.100m.min.dat"]

    assert "hfd.picarro.1minute.100m.min_ch4" in results
    assert "hfd.picarro.1minute.100m.min_co2" in results

