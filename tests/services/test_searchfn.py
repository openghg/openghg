import pytest

from openghg.client import Process, Search
from openghg.objectstore import get_local_bucket

from helpers import get_datapath, glob_files


@pytest.fixture(scope="session")
def read_data(authenticated_user):
    get_local_bucket(empty=True)

    process = Process(service_url="openghg")

    data = get_datapath(filename="capegrim-medusa.18.C", data_type="GC")
    precision = get_datapath(filename="capegrim-medusa.18.precisions.C", data_type="GC")

    gc_files = (data, precision)

    process.process_files(
        user=authenticated_user,
        files=gc_files,
        site="cgo",
        network="AGAGE",
        data_type="GCWERKS",
        openghg_url="openghg",
        storage_url="storage",
    )

    bsd_files = glob_files(search_str="bsd.picarro.1minute", data_type="CRDS")
    hfd_files = glob_files(search_str="hfd.picarro.1minute", data_type="CRDS")

    process.process_files(
        user=authenticated_user,
        files=bsd_files,
        site="bsd",
        network="DECC",
        data_type="CRDS",
        openghg_url="openghg",
        storage_url="storage",
    )

    process.process_files(
        user=authenticated_user,
        files=hfd_files,
        site="hfd",
        network="DECC",
        data_type="CRDS",
        openghg_url="openghg",
        storage_url="storage",
    )


def test_search(read_data):
    search = Search(service_url="openghg")

    species = "co2"
    site = "bsd"

    results = search.search(species=species, site=site, inlet="248m")

    raw_results = results.raw()

    assert len(raw_results["bsd"]["co2"]["248m"]["keys"]) == 7

    assert raw_results["bsd"]["co2"]["248m"]["metadata"] == {
        "site": "bsd",
        "instrument": "picarro",
        "sampling_period": "60",
        "inlet": "248m",
        "port": "8",
        "type": "air",
        "network": "decc",
        "species": "co2",
        "scale": "wmo-x2007",
        "data_type": "timeseries",
    }

    results = search.search(site="hfd", species="co", skip_ranking=True)

    raw_results = results.raw()

    assert raw_results["hfd"]["co"]["50m"]["metadata"] == {
        "site": "hfd",
        "instrument": "picarro",
        "sampling_period": "60",
        "inlet": "50m",
        "port": "9",
        "type": "air",
        "network": "decc",
        "species": "co",
        "scale": "wmo-x2014a",
        "data_type": "timeseries",
    }

    assert raw_results["hfd"]["co"]["100m"]["metadata"] == {
        "site": "hfd",
        "instrument": "picarro",
        "sampling_period": "60",
        "inlet": "100m",
        "port": "10",
        "type": "air",
        "network": "decc",
        "species": "co",
        "scale": "wmo-x2014a",
        "data_type": "timeseries",
    }

    assert len(raw_results["hfd"]["co"]["50m"]["keys"]) == 3
    assert len(raw_results["hfd"]["co"]["100m"]["keys"]) == 6

    results = search.search(species=["NF3"], site="CGO", skip_ranking=True)

    raw_results = results.raw()

    assert raw_results["cgo"]["nf3"]["70m"]["metadata"] == {
        "instrument": "medusa",
        "site": "cgo",
        "network": "agage",
        "sampling_period": "1200",
        "species": "nf3",
        "units": "ppt",
        "scale": "sio-12",
        "inlet": "70m",
        "data_type": "timeseries",
    }

    assert len(raw_results["cgo"]["nf3"]["70m"]["keys"]) == 1
