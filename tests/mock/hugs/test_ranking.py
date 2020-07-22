import pytest
import os

from HUGS.Client import RankSources
from Acquire.Client import Service

@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("tmp_searchfn")
    return str(d)


@pytest.fixture(scope="session")
def load_crds(authenticated_user):
    hugs = Service(service_url="hugs")
    _ = hugs.call_function(function="clear_datasources", args={})

    def test_folder(filename):
        dir_path = os.path.dirname(__file__)
        test_folder = "../../../tests/data/search_data"
        return os.path.join(dir_path, test_folder, filename)

    files = [
        "bsd.picarro.1minute.108m.min.dat",
        "hfd.picarro.1minute.100m.min.dat",
        "tac.picarro.1minute.100m.min.dat",
    ]
    filepaths = [test_folder(f) for f in files]

    process = Process(service_url="hugs")

    process.process_files(
        user=authenticated_user,
        files=filepaths,
        data_type="CRDS",
        hugs_url="hugs",
        storage_url="storage",
    )


# Need authenticated_user here to allow
def test_get_sources(crds, authenticated_user):
    r = RankSources(service_url="hugs")
    sources = r.get_sources(site="hfd", species="co2", data_type="CRDS")
    print(sources)
