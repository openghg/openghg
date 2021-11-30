import pytest

from openghg.client import Process
from openghg.objectstore import get_local_bucket

from helpers import get_datapath


@pytest.fixture(autouse=True)
def run_before_tests():
    get_local_bucket(empty=True)


def test_process_CRDS_files(authenticated_user):
    get_local_bucket(empty=True)

    service_url = "openghg"

    bsd_file = get_datapath(filename="bsd.picarro.1minute.248m.min.dat", data_type="CRDS")

    process = Process(service_url=service_url)

    response = process.process_files(
        user=authenticated_user,
        files=bsd_file,
        data_type="CRDS",
        site="bsd",
        network="DECC",
        openghg_url="openghg",
        storage_url="storage",
    )

    processed_species = response["processed"]["bsd.picarro.1minute.248m.min.dat"]

    assert sorted(list(processed_species.keys())) == ["ch4", "co", "co2"]


def test_process_CRDS_incorrect_args(authenticated_user):
    hfd_file = get_datapath(filename="hfd.picarro.1minute.100m.min.dat", data_type="CRDS")

    service_url = "openghg"
    process = Process(service_url=service_url)

    response = process.process_files(
        user=authenticated_user,
        files=hfd_file,
        data_type="CRDS",
        site="bsd",
        network="DECC",
        openghg_url="openghg",
        storage_url="storage",
    )

    assert "ValueError: Site mismatch between passed site code and that read from filename." in (response["hfd.picarro.1minute.100m.min.dat"])

    with pytest.raises(TypeError):
        response = process.process_files(
            user=authenticated_user,
            files=hfd_file,
            data_type="GCWERKS",
            site="bsd",
            network="DECC",
            openghg_url="openghg",
            storage_url="storage",
        )

    response = process.process_files(
        user=authenticated_user,
        files=hfd_file,
        data_type="CRDS",
        site="hfd",
        network="DECC",
        openghg_url="openghg",
        storage_url="storage",
    )

    processed_species = response["processed"]["hfd.picarro.1minute.100m.min.dat"]

    assert sorted(list(processed_species.keys())) == ["ch4", "co", "co2"]


def test_process_GCWERKS_files(authenticated_user):
    service_url = "openghg"

    # Get the precisin filepath
    data = get_datapath("capegrim-medusa.18.C", "GC")
    precisions = get_datapath("capegrim-medusa.18.precisions.C", "GC")

    filepaths = [(data, precisions)]

    process = Process(service_url=service_url)

    response = process.process_files(
        user=authenticated_user,
        files=filepaths,
        data_type="GCWERKS",
        site="cgo",
        network="AGAGE",
        openghg_url="openghg",
        storage_url="storage",
        instrument="medusa",
    )

    cgo_response = response["processed"]["capegrim-medusa.18.C"]

    partial_expected_keys = ['c2cl4_70m', 'c2f6_70m', 'c2h2_70m', 'c2h6_70m', 'c2hcl3_70m']

    assert len(cgo_response.keys()) == 56
    assert sorted(cgo_response.keys())[:5] == partial_expected_keys
