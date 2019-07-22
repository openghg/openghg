import pytest
import os
from pandas import read_json, Timestamp

from HUGS.Client import Retrieve, Search
from HUGS.Modules import CRDS
from HUGS.ObjectStore import get_local_bucket

@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("")
    return str(d)

# Seems like a lot to be doing this before each test? Alternative?
@pytest.fixture(autouse=True)
def crds():
    _ = get_local_bucket(empty=True)
    crds = CRDS.create()
    crds.save()

def test_retrieve(authenticated_user):
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    crds = CRDS.read_file(filepath)

    search_term = "co"
    location = "bsd"
    data_type = "CRDS"

    search_obj = Search(service_url="hugs")

    search_results = search_obj.search(search_terms=search_term, locations=location, data_type=data_type)

    retrieve_obj = Retrieve(service_url="hugs")
    data = retrieve_obj.retrieve(keys=search_results)

    # Here we get some JSON data that can be converted back into a DataFrame
    df = read_json(data[0])

    head = df.head(1)

    assert head.first_valid_index() == Timestamp("2014-01-30 10:52:30")
    assert head["co count"].iloc[0] == 204.62
    assert head["co stdev"].iloc[0] == 6.232
    assert head["co n_meas"].iloc[0] == 26
