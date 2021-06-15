import pytest
import os

from openghg.client import RankSources, Process
from openghg.objectstore import get_local_bucket


@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("tmp_searchfn")
    return str(d)


@pytest.fixture(scope="session")
def load_crds(authenticated_user):
    get_local_bucket(empty=True)

    def test_folder(filename):
        dir_path = os.path.dirname(__file__)
        test_folder = "../../../tests/data/proc_test_data/CRDS"
        return os.path.join(dir_path, test_folder, filename)

    files = [
        "hfd.picarro.1minute.100m.min.dat",
        "hfd.picarro.1minute.50m.min.dat",
    ]
    filepaths = [test_folder(f) for f in files]

    process = Process(service_url="openghg")

    process.process_files(
        user=authenticated_user,
        files=filepaths,
        site="hfd",
        network="DECC",
        data_type="CRDS",
        openghg_url="openghg",
        storage_url="storage",
    )


def test_set_ranking(authenticated_user, load_crds):
    r = RankSources(service_url="openghg")

    response = r.get_sources(site="hfd", species="co2")

    print(response)



