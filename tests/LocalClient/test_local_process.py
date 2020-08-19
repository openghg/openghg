import os
from HUGS.LocalClient import process_folder, process_files
from HUGS.ObjectStore import get_local_bucket


def test_process_files():
    get_local_bucket(empty=True)
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filename = "hfd.picarro.1minute.100m.min.dat"
    filepath = os.path.join(dir_path, test_data, filename)

    results = process_files(files=filepath, site="hfd", instrument="picarro", network="DECC", data_type="CRDS")

    assert "hfd.picarro.1minute.100m.min_ch4" in results
    assert "hfd.picarro.1minute.100m.min_co2" in results


def test_process_folder():
    get_local_bucket(empty=True)
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    folder_path = os.path.join(dir_path, test_data)

    results = process_folder(folder_path=folder_path, data_type="CRDS")

    expected_keys = sorted(['tac.picarro.1minute.100m.test_ch4', 'tac.picarro.1minute.100m.test_co2', 'hfd.picarro.1minute.50m.min_ch4', 
                    'hfd.picarro.1minute.50m.min_co2', 'hfd.picarro.1minute.50m.min_co', 'hfd.picarro.1minute.100m.min_ch4', 
                    'hfd.picarro.1minute.100m.min_co2', 'hfd.picarro.1minute.100m.min_co', 'tac.picarro.1minute.100m.min_ch4', 
                    'tac.picarro.1minute.100m.min_co2', 'bsd.picarro.1minute.248m_ch4', 'bsd.picarro.1minute.248m_co2', 
                    'bsd.picarro.1minute.248m_co'])

    assert sorted(list(results.keys())) == expected_keys
