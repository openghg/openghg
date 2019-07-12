import pytest
import os

from HUGS.Client import ListObjects

@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("")
    return str(d)

def test_listobjects(tempdir):
    listobj = ListObjects(service_url="hugs")

    results = listobj.get_list()

    assert(False)
