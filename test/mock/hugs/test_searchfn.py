import pytest
import os
import uuid

from HUGS.Client import ListObjects
from HUGS.Client import Search
from HUGS.Client import Process

from HUGS.Modules import CRDS
from HUGS.ObjectStore import get_local_bucket
from Acquire.Client import User, Drive, Service, StorageCreds, PAR, Authorisation

@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("")
    return str(d)

@pytest.fixture(scope="session")
def load_crds(authenticated_user):
    hugs = Service(service_url="hugs")
    _ = hugs.call_function(function="clear_datasources", args={})

    def test_folder(filename):
        dir_path = os.path.dirname(__file__)
        test_folder = "../../../test/data/search_data"
        return os.path.join(dir_path, test_folder, filename)

    files = ["bsd.picarro.1minute.108m.min.dat", "hfd.picarro.1minute.100m.min.dat", "tac.picarro.1minute.100m.min.dat"]
    filepaths = [test_folder(f) for f in files]

    process = Process(service_url="hugs") 

    response = process.process_files(user=authenticated_user, files=filepaths, data_type="CRDS", 
                                        hugs_url="hugs", storage_url="storage")


def test_search_bsd(crds):
    search = Search(service_url="hugs")

    search_term = "co"
    location = "bsd"
    data_type = "CRDS"

    results = search.search(search_terms=search_term, locations=location, data_type=data_type)

    assert len(results) == 1
    assert sorted(results["bsd_co"])[0].split("/")[-1] == "2014-01-30T10:52:30_2014-01-30T14:20:30"

def test_search_multispecies_singlesite(load_crds):
    search = Search(service_url="hugs")

    search_term = ["co", "co2"]
    location = "bsd"
    data_type = "CRDS"

    results = search.search(search_terms=search_term, locations=location, data_type=data_type)

    assert len(results["bsd_co"]) == 6
    assert len(results["bsd_co2"]) == 6

    assert sorted(results["bsd_co"])[0].split("/")[-1] == "2014-01-30T13:33:30_2014-12-31T22:23:30"
    assert sorted(results["bsd_co"])[5].split("/")[-1] == "2019-01-01T04:44:30_2019-07-04T04:23:30"

    assert sorted(results["bsd_co"])[0].split("/")[-1] == "2014-01-30T13:33:30_2014-12-31T22:23:30"
    assert sorted(results["bsd_co"])[5].split("/")[-1] == "2019-01-01T04:44:30_2019-07-04T04:23:30"


def test_search_multisite_co(load_crds):
    search = Search(service_url="hugs")

    search_term = "co"
    location = ["bsd", "hfd", "tac"]
    data_type = "CRDS"

    results = search.search(search_terms=search_term, locations=location, data_type=data_type)

    assert len(results) == 2

    assert len(results["bsd_co"]) == 6
    assert len(results["hfd_co"]) == 7

    assert sorted(results["bsd_co"])[0].split("/")[-1] == "2014-01-30T13:33:30_2014-12-31T22:23:30"
    assert sorted(results["bsd_co"])[5].split("/")[-1] == "2019-01-01T04:44:30_2019-07-04T04:23:30"

    assert sorted(results["hfd_co"])[0].split("/")[-1] == "2013-11-20T20:02:30_2013-12-31T22:54:30"
    assert sorted(results["hfd_co"])[6].split("/")[-1] == "2019-01-01T02:55:30_2019-07-04T21:29:30" 


def test_search_multiplesite_multiplespecies(load_crds):
    search = Search(service_url="hugs")

    search_term = ["ch4", "co2"]
    location = ["bsd", "hfd", "tac"]
    data_type = "CRDS"

    results = search.search(search_terms=search_term, locations=location, data_type=data_type)

    expected_keys = set(["bsd_ch4","bsd_co2","tac_co2","tac_ch4","hfd_ch4","hfd_co2"])

    assert set(results.keys()) == expected_keys

    assert len(results["bsd_ch4"]) == 6
    assert len(results["bsd_co2"]) == 6

    assert len(results["hfd_ch4"]) == 7
    assert len(results["hfd_co2"]) == 7

    assert len(results["tac_ch4"]) == 8
    assert len(results["tac_co2"]) == 8