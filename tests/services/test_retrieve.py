import pytest
from pandas import Timestamp
from openghg.client import Search, Process, Retrieve
from openghg.objectstore import get_local_bucket
from helpers import get_datapath, attributes_checker_get_obs


@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("")
    return str(d)


@pytest.fixture(autouse=True)
def crds(authenticated_user):
    get_local_bucket(empty=True)
    service_url = "openghg"
    bsd_file = get_datapath(
        filename="bsd.picarro.1minute.248m.min.dat", data_type="CRDS"
    )
    process = Process(service_url=service_url)

    process.process_files(
        user=authenticated_user,
        files=bsd_file,
        data_type="CRDS",
        site="bsd",
        network="DECC",
        openghg_url="openghg",
        storage_url="storage",
    )


def test_retrieve(authenticated_user):
    search = Search(service_url="openghg")

    species = "co2"
    site = "bsd"

    results = search.search(species=species, site=site, inlet="248m")

    keys = results.keys(site="bsd", species="co2", inlet="248m")

    retrieve = Retrieve(service_url="openghg")

    to_retrieve = {"bsd_co2_248m": keys}

    data = retrieve.retrieve(keys=to_retrieve)

    ds = data["bsd_co2_248m"]

    attrs = ds.attrs

    attributes_checker_get_obs(attrs=attrs, species="co2")

    assert ds["time"][0] == Timestamp("2014-01-30T11:12:30")
    assert ds["co2"][0] == 409.55
