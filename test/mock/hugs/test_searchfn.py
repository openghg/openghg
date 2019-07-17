import pytest
import os
import uuid

from HUGS.Client import ListObjects
from HUGS.Client import Search

from HUGS.Modules import CRDS
from HUGS.ObjectStore import get_local_bucket

@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("")
    return str(d)

def test_search(authenticated_user, tempdir):
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)
    _ = get_local_bucket(empty=True)

    crds = CRDS.create()
    crds.save()

    crds = CRDS.read_file(filepath)

    search = Search(service_url="hugs")

    search_term = "co"
    location = "bsd"
    data_type = "CRDS"

    results = search.search(search_terms=search_term, locations=location, data_type=data_type)

    print(results)

    assert len(results) == 1
