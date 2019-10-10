import datetime
import os
import pytest
# import matplotlib.pyplot as plt

from HUGS.Modules import CRDS, GC
from HUGS.Modules import Datasource
from HUGS.ObjectStore import get_local_bucket, get_object_names
from HUGS.Processing import in_daterange, key_to_daterange
from HUGS.Processing import recombine_sections, search
from HUGS.Util import get_datetime, load_object
                            

from Acquire.ObjectStore import datetime_to_string
from Acquire.ObjectStore import datetime_to_datetime

# Create the CRDS object
# @pytest.fixture(autouse=True)
# def create_crds():
#     # prepare something ahead 
#     crds = CRDS.create()
#     crds.save()

@pytest.fixture(scope="session")
def crds_obj():
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    return CRDS.read_file(filepath, source_name="bsd.picarro.1minute.248m")

@pytest.fixture(scope="session")
def crds_read():
    bucket = get_local_bucket(empty=True)
    test_data = "../data/search_data"
    folder_path = os.path.join(os.path.dirname(__file__), test_data)
    CRDS.read_folder(folder_path=folder_path)


def test_load_object(crds_obj):
    bucket = get_local_bucket()
    crds_obj.save(bucket)
    uuid = crds_obj.uuid()
    class_name = "crds"
    obj = load_object(class_name=class_name)

    assert isinstance(obj, CRDS)
    assert obj.uuid() == crds_obj.uuid()


def test_location_search(crds_read):
    search_terms = ["co2", "ch4"]
    locations = ["bsd", "hfd", "tac"]

    data_type = "CRDS"
    start = None  # get_datetime(year=2016, month=1, day=1)
    end = None  # get_datetime(year=2017, month=1, day=1)

    results = search(search_terms=search_terms, locations=locations, data_type=data_type, require_all=False, 
                    start_datetime=start, end_datetime=end)

    assert "bsd_co2" in results
    assert "hfd_co2" in results
    assert "tac_co2" in results
    assert "bsd_ch4" in results
    assert "hfd_ch4" in results
    assert "tac_ch4" in results

    assert len(results["bsd_co2"]) == 6
    assert len(results["hfd_co2"]) == 7
    assert len(results["tac_co2"]) == 8
    assert len(results["bsd_ch4"]) == 6
    assert len(results["hfd_ch4"]) == 7
    assert len(results["tac_ch4"]) == 8

def test_search_no_search_terms(crds_read):
    data_type = "CRDS"
    search_terms = []
    locations = ["bsd"]

    results = search(search_terms=search_terms, locations=locations, data_type=data_type, require_all=False, 
                    start_datetime=None, end_datetime=None)

    assert len(results["bsd_ch4"]) == 6
    assert len(results["bsd_co2"]) == 6
    assert len(results["bsd_co"]) == 6

def test_search_no_locations(crds_read):
    data_type = "CRDS"
    search_terms = ["ch4"]
    locations = []

    results = search(search_terms=search_terms, locations=locations, data_type=data_type, require_all=False, 
                    start_datetime=None, end_datetime=None)

    assert len(results["bsd_ch4"]) == 6
    assert len(results["hfd_ch4"]) == 7
    assert len(results["tac_ch4"]) == 8

def test_search_datetimes():
    data_type = "CRDS"
    search_terms = ["co2"]
    locations = ["bsd"]

    start = get_datetime(year=2016, month=1, day=1)
    end = get_datetime(year=2017, month=1, day=1)

    results = search(search_terms=search_terms, locations=locations, data_type=data_type, require_all=False, 
                    start_datetime=start, end_datetime=end)

    assert results["bsd_co2"][0].split("/")[-1] == "2016-01-19T17:17:30_2016-12-31T23:52:30"

def test_search_require_all():
    data_type = "CRDS"
    search_terms = ["co2", "picarro", "108m"]
    locations = ["bsd"]

    start = get_datetime(year=2016, month=1, day=1)
    end = get_datetime(year=2017, month=1, day=1)

    results = search(search_terms=search_terms, locations=locations, data_type=data_type, require_all=True, 
                    start_datetime=start, end_datetime=end)

    assert len(results["bsd_108m_co2_picarro"]) == 3



















