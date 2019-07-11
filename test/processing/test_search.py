import datetime
import os
import pytest
import matplotlib.pyplot as plt

from HUGS.Modules import CRDS, GC
from HUGS.Modules import Datasource
from HUGS.ObjectStore import get_local_bucket, get_object_names
from HUGS.Processing import in_daterange, key_to_daterange
from HUGS.Processing import recombine_sections, search
from HUGS.Util import get_datetime, load_object
                            

from Acquire.ObjectStore import datetime_to_string
from Acquire.ObjectStore import datetime_to_datetime

# Create the CRDS object
@pytest.fixture(scope="session", autouse=True)
def create_crds():
    # prepare something ahead 
    crds = CRDS.create()
    crds.save()

@pytest.fixture(scope="session")
def crds():
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    return CRDS.read_file(filepath)


def test_load_object(crds):
    bucket = get_local_bucket()
    crds.save(bucket)
    uuid = crds.uuid()
    class_name = "crds"
    obj = load_object(class_name=class_name)

    assert isinstance(obj, CRDS)
    assert obj.uuid() == crds.uuid()


def test_gas_search_CRDS():
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)  

    _ = get_local_bucket(empty=True)

    crds = CRDS.read_file(filepath)

    gas_name = "co"
    data_type = "CRDS"

    keys = gas_search(species=gas_name, data_type=data_type)

    assert len(keys) == 1


def test_gas_search_CRDS_two():
    filename = "hfd.picarro.1minute.100m_min.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    _ = get_local_bucket(empty=True)

    crds = CRDS.read_file(filepath)

    gas_name = "co"
    data_type = "CRDS"

    keys = gas_search(species=gas_name, data_type=data_type)

    assert len(keys) == 7


def test_search_GC():
    data_filename = "capegrim-medusa.18.C"
    precision_filename = "capegrim-medusa.18.precisions.C"

    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/GC"
    data_filepath = os.path.join(dir_path, test_data, data_filename)
    precision_filepath = os.path.join(dir_path, test_data, precision_filename)

    _ = get_local_bucket(empty=True)

    gc = GC.create()
    gc.save()

    gc = GC.read_file(data_filepath=data_filepath, precision_filepath=precision_filepath)

    search_term = "NF3"
    data_type = "GC"

    results = search(search_terms=search_term, data_type=data_type)

    assert len(results[search_term]) == 1


def test_general_search():
    # Test a more general search function
    filename = "hfd.picarro.1minute.100m_min.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    _ = get_local_bucket(empty=True)

    crds = CRDS.read_file(filepath)

    search_term = "co"
    data_type = "CRDS"

    results = search(search_terms=search_term, data_type=data_type)

    assert len(results[search_term]) == 7    


def test_general_search_multiple_terms():
     # Test a more general search function
    filename = "hfd.picarro.1minute.100m_min.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    _ = get_local_bucket(empty=True)

    crds = CRDS.read_file(filepath)

    search_terms = ["co", "co2", "ch4"]
    data_type = "CRDS"

    results = search(search_terms=search_terms, data_type=data_type)

    assert len(results["co2"]) == 7
    assert len(results["co"]) == 7
    assert len(results["ch4"]) == 7

    assert len(results["co"]) == len(set(results["co"]))
    assert len(results["co2"]) == len(set(results["co2"]))
    assert len(results["ch4"]) == len(set(results["ch4"]))


def test_search_all_terms():
    filename_hfd = "hfd.picarro.1minute.100m_min.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename_hfd)

    _ = get_local_bucket(empty=True)

    crds = CRDS.read_file(filepath)

    # Items should contain all these terms
    search_terms  = ["co", "hfd", "picarro"]
    data_type = "CRDS"

    results = search(search_terms=search_terms, data_type=data_type, require_all=True)

    assert len(results["co_hfd_picarro"]) == 7


def test_three_sites():
    # Here can return a single key for each search term
    # How to seach for 3 different sites
    # bilsdale, heathfield, tacolneston
    # Between 2016 - 2018
    # search terms bsd, hfd, tac
    bucket = get_local_bucket(empty=True)

    test_data = "../data/search_data"
    folder_path = os.path.join(os.path.dirname(__file__), test_data)
    CRDS.read_folder(folder_path=folder_path)

    prefix = "datasource"
    objs = get_object_names(bucket=bucket, prefix=prefix)

    datasources = [Datasource.load(key=key) for key in objs]

    search_terms = ["bsd", "hfd", "tac"]

    # Search sites for a single gas - how?
    data_type = "CRDS"
    start = get_datetime(year=2016, month=1, day=1)
    end = get_datetime(year=2017, month=1, day=1)

    results = search(search_terms=search_terms, data_type=data_type, require_all=False, start_datetime=start, end_datetime=end)

    # Get the results for each of the keys
    recombined_sections = recombine_sections(data_keys=results)

    hdf_ch4 = recombined_sections["hfd_ch4"]
    bsd_ch4 = recombined_sections["bsd_ch4"]
    tac_ch4 = recombined_sections["tac_ch4"]

    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.plot(hdf_ch4.index.values, hdf_ch4["ch4 count"], label = "hdf ch4", linewidth = 1)
    ax.plot(bsd_ch4.index.values, bsd_ch4["ch4 count"], label = "bsd ch4", linewidth = 1)
    ax.plot(tac_ch4.index.values, tac_ch4["ch4 count"], label = "tac ch4", linewidth = 1)
    ax.legend()

    plt.show()
    
    assert False
