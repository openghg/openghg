import datetime
import os
import pytest
# import matplotlib.pyplot as plt

from HUGS.Modules import CRDS, GC, Footprint
from HUGS.Modules import Datasource
from HUGS.ObjectStore import get_local_bucket, get_object_names
from HUGS.Processing import in_daterange, key_to_daterange
from HUGS.Processing import recombine_sections, search
from HUGS.Util import get_datetime, load_object, get_datetime_epoch, get_datetime_now
                            

from Acquire.ObjectStore import datetime_to_string
from Acquire.ObjectStore import datetime_to_datetime

# Create the CRDS object
# @pytest.fixture(autouse=True)
# def create_crds():
#     # prepare something ahead 
#     crds = CRDS.create()
#     crds.save()


@pytest.fixture(scope="session")
def gc_obj():
    bucket = get_local_bucket(empty=False)
    data_file = "capegrim-medusa.18.C"
    prec_file = "capegrim-medusa.18.precisions.C"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/GC"
    data_filepath = os.path.join(dir_path, test_data, data_file)
    prec_filepath = os.path.join(dir_path, test_data, prec_file)

    GC.read_file(data_filepath=data_filepath, precision_filepath=prec_filepath, site="capegrim", source_name="capegrim-medusa.18", instrument_name="medusa")

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

def test_search_GC():
    locations = []
    data_type = "GC"
    start = None
    end = None

    bucket = get_local_bucket(empty=False)
    data_file = "capegrim-medusa.18.C"
    prec_file = "capegrim-medusa.18.precisions.C"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/GC"
    data_filepath = os.path.join(dir_path, test_data, data_file)
    prec_filepath = os.path.join(dir_path, test_data, prec_file)

    GC.read_file(data_filepath=data_filepath, precision_filepath=prec_filepath, site="CGO", 
                                source_name="capegrim-medusa", instrument_name="medusa")

    results = search(search_terms=["NF3"], locations=locations, data_type=data_type, require_all=False, 
                        start_datetime=start, end_datetime=end)

    nf3_results = results["capegrim_NF3_75m_4"]

    metadata = {'site': 'capegrim', 'instrument': 'medusa', 
                'species': 'nf3', 'units': 'ppt', 'scale': 'sio-12', 
                'inlet': '75m_4', 'data_type': 'timeseries'}

    assert nf3_results["metadata"] == metadata
    assert nf3_results["start_date"] == "2018-01-01-02:24:00+00:00"
    assert nf3_results["end_date"] == "2018-01-31-23:33:00+00:00"


def test_location_search(crds_read):
    search_terms = ["co2", "ch4"]
    locations = ["bsd", "hfd", "tac"]

    data_type = "CRDS"
    start = None  # get_datetime(year=2016, month=1, day=1)
    end = None  # get_datetime(year=2017, month=1, day=1)

    results = search(search_terms=search_terms, locations=locations, data_type=data_type, require_all=False, 
                    start_datetime=start, end_datetime=end)

    results_list = sorted(list(results.keys()))

    expected = sorted(['bsd_co2_108m', 'hfd_co2_100m', 'tac_co2_100m', 'bsd_ch4_108m', 'hfd_ch4_100m', 'tac_ch4_100m'])

    assert results_list == expected
    
    assert len(results["bsd_co2_108m"]["keys"]) == 23
    assert len(results["hfd_co2_100m"]["keys"]) == 25
    assert len(results["tac_co2_100m"]["keys"]) == 30
    assert len(results["bsd_ch4_108m"]["keys"]) == 23
    assert len(results["hfd_ch4_100m"]["keys"]) == 25
    assert len(results["tac_ch4_100m"]["keys"]) == 30

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

def test_search_footprints():
    bucket = get_local_bucket(empty=True)
    test_data = "../data/emissions"
    filename = "WAO-20magl_EUROPE_201306_downsampled.nc"
    filepath = os.path.join(os.path.dirname(__file__), test_data, filename)
    metadata = {"name": "WAO-20magl_EUROPE"}
    source_name = "WAO-20magl_EUROPE"
    datasource_uuids = Footprint.read_file(filepath=filepath, metadata=metadata, source_name=source_name)

    data_type = "footprint"
    search_terms = []
    locations = []

    start = get_datetime_epoch()
    end = get_datetime_now()

    results = search(search_terms=search_terms, locations=locations, data_type=data_type, start_datetime=start, end_datetime=end)

    assert len(results["footprints"]) == 1













