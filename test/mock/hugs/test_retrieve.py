import pytest
import os
from pandas import read_json, Timestamp

from HUGS.Client import Retrieve, Search
from HUGS.Modules import CRDS
from HUGS.ObjectStore import get_local_bucket, get_object_names
from Acquire.Client import User, Drive, Service, StorageCreds, PAR, Authorisation

@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("")
    return str(d)

# Seems like a lot to be doing this before each test? Alternative?
@pytest.fixture(autouse=True)
def crds(authenticated_user):
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


def test_retrieve(authenticated_user, crds):

    search_term = "co"
    location = "bsd"
    data_type = "CRDS"

    search_obj = Search(service_url="hugs")

    search_results = search_obj.search(search_terms=search_term, locations=location, data_type=data_type)

    retrieve_obj = Retrieve(service_url="hugs")
    data = retrieve_obj.retrieve(keys=search_results)

    # Here we get some JSON data that can be converted back into a DataFrame
    df = read_json(data["bsd_co"])

    head = df.head(1)

    assert head.first_valid_index() == Timestamp("2014-01-30 10:52:30")
    assert head["co count"].iloc[0] == 204.62
    assert head["co stdev"].iloc[0] == 6.232
    assert head["co n_meas"].iloc[0] == 26
