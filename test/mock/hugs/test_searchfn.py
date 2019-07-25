import pytest
import os
import uuid

from HUGS.Client import ListObjects
from HUGS.Client import Search

from HUGS.Modules import CRDS
from HUGS.ObjectStore import get_local_bucket
from Acquire.Client import User, Drive, Service, StorageCreds, PAR, Authorisation

@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("")
    return str(d)

@pytest.fixture(autouse=True)
def crds(authenticated_user):
    # Empty the local bucket
    _ = get_local_bucket(empty=True)
    creds = StorageCreds(user=authenticated_user, service_url="storage")
    drive = Drive(creds=creds, name="test_drive")
    filepath = os.path.join(os.path.dirname(__file__), "../../../test/data/proc_test_data/CRDS/bsd.picarro.1minute.248m.dat")
    filemeta = drive.upload(filepath)

    par = PAR(location=filemeta.location(), user=authenticated_user)

    hugs = Service(service_url="hugs")
    par_secret = hugs.encrypt_data(par.secret())

    auth = Authorisation(resource="process", user=authenticated_user)

    args = {"authorisation": auth.to_data(),
            "par": {"data": par.to_data()},
            "par_secret": {"data": par_secret},
            "data_type": "CRDS"}

    response = hugs.call_function(function="process", args=args)


def test_search_bsd(crds):
    search = Search(service_url="hugs")

    search_term = "co"
    location = "bsd"
    data_type = "CRDS"

    results = search.search(search_terms=search_term, locations=location, data_type=data_type)

    assert len(results) == 1
    assert sorted(results["bsd_co"])[0].split("/")[-1] == "2014-01-30T10:52:30_2014-01-30T14:20:30"


def test_search_multiple(crds):
    dir_path = os.path.dirname(__file__)
    test_data = "../../data/search_data"
    filepath = os.path.join(dir_path, test_data)

    crds = CRDS.read_folder(filepath)

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