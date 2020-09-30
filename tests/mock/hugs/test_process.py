import os
import pytest
from pathlib import Path

from Acquire.Client import PAR, Authorisation, Drive, Service, StorageCreds

from HUGS.Client import Process
from HUGS.Modules import CRDS, GCWERKS
from HUGS.ObjectStore import get_local_bucket


def get_datapath(filename, data_type):
    """ Get the path of a file in the tests directory 

        Returns:
            pathlib.Path
    """
    return (
        Path(__file__)
        .resolve()
        .parent.parent.parent.joinpath(
            "data", "proc_test_data", data_type.upper(), filename
        )
    )


@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("tmp_process")
    return str(d)


@pytest.fixture(autouse=True)
def run_before_tests():
    get_local_bucket(empty=True)


def get_test_folder(filename):
    dir_path = os.path.dirname(__file__)
    test_folder = "../../../tests/data/search_data"
    return os.path.join(dir_path, test_folder, filename)


@pytest.mark.skip(reason="Need to fix dependence on Acquire")
def test_process_CRDS_files(authenticated_user):
    service_url = "hugs"

    files = [
        "bsd.picarro.1minute.108m.min.dat",
        "hfd.picarro.1minute.100m.min.dat",
        "tac.picarro.1minute.100m.min.dat",
    ]
    filepaths = [get_test_folder(f) for f in files]

    # Make sure don't have the temporary files
    for f in files:
        Path(f"/tmp/{f}").unlink(missing_ok=True)

    process = Process(service_url=service_url)

    response = process.process_files(
        user=authenticated_user,
        files=filepaths,
        data_type="CRDS",
        hugs_url="hugs",
        storage_url="storage",
    )

    assert len(response["bsd.picarro.1minute.108m.min.dat"]) == 3
    assert len(response["hfd.picarro.1minute.100m.min.dat"]) == 3
    assert len(response["tac.picarro.1minute.100m.min.dat"]) == 2


@pytest.mark.skip(reason="Need to fix dependence on Acquire")
def test_process_GC_files(authenticated_user):
    service_url = "hugs"

    # Get the precisin filepath
    data = get_datapath("capegrim-medusa.18.C", "GC")
    precisions = get_datapath("capegrim-medusa.18.precisions.C", "GC")

    filepaths = [(data, precisions)]

    process = Process(service_url=service_url)

    # TODO - work out a cleaner way to do this
    Path("/tmp/capegrim-medusa.18.C").unlink(missing_ok=True)
    Path("/tmp/capegrim-medusa.18.precisions.C").unlink(missing_ok=True)

    response = process.process_files(
        user=authenticated_user,
        files=filepaths,
        data_type="GCWERKS",
        hugs_url="hugs",
        storage_url="storage",
        instrument="medusa",
        site="capegrim",
    )

    expected_keys = [
        "capegrim-medusa.18_C4F10",
        "capegrim-medusa.18_C6F14",
        "capegrim-medusa.18_CCl4",
        "capegrim-medusa.18_CF4",
        "capegrim-medusa.18_CFC-11",
    ] 

    assert len(response["capegrim-medusa.18.C"].keys()) == 56
    assert sorted(response["capegrim-medusa.18.C"].keys())[:5] == expected_keys


@pytest.mark.skip(reason="Need to fix dependence on Acquire")
def test_process_CRDS(authenticated_user, tempdir):
    creds = StorageCreds(user=authenticated_user, service_url="storage")
    drive = Drive(creds=creds, name="test_drive")
    filepath = os.path.join(
        os.path.dirname(__file__),
        "../../../tests/data/proc_test_data/CRDS/bsd.picarro.1minute.248m.dat",
    )
    filemeta = drive.upload(filepath)

    Path("/tmp/bsd.picarro.1minute.248m.dat").unlink(missing_ok=True)

    par = PAR(location=filemeta.location(), user=authenticated_user)

    hugs = Service(service_url="hugs")
    par_secret = hugs.encrypt_data(par.secret())

    auth = Authorisation(resource="process", user=authenticated_user)

    args = {
        "authorisation": auth.to_data(),
        "par": {"data": par.to_data()},
        "par_secret": {"data": par_secret},
        "data_type": "CRDS",
        "source_name": "bsd.picarro.1minute.248m",
    }

    response = hugs.call_function(function="process", args=args)

    expected_keys = [
        "bsd.picarro.1minute.248m_ch4",
        "bsd.picarro.1minute.248m_co",
        "bsd.picarro.1minute.248m_co2",
    ]

    results = response["results"]["bsd.picarro.1minute.248m.dat"]

    return False

    assert sorted(results.keys()) == expected_keys


@pytest.mark.skip(reason="Need to fix dependence on Acquire")
def test_process_GC(authenticated_user, tempdir):
    creds = StorageCreds(user=authenticated_user, service_url="storage")
    drive = Drive(creds=creds, name="test_drive")
    data_filepath = os.path.join(
        os.path.dirname(__file__),
        "../../../tests/data/proc_test_data/GC/capegrim-medusa.18.C",
    )
    precision_filepath = os.path.join(
        os.path.dirname(__file__),
        "../../../tests/data/proc_test_data/GC/capegrim-medusa.18.precisions.C",
    )

    Path("/tmp/capegrim-medusa.18.C").unlink(missing_ok=True)
    Path("/tmp/capegrim-medusa.18.precisions.C").unlink(missing_ok=True)

    data_meta = drive.upload(data_filepath)
    precision_meta = drive.upload(precision_filepath)

    data_par = PAR(location=data_meta.location(), user=authenticated_user)
    precision_par = PAR(location=precision_meta.location(), user=authenticated_user)

    hugs = Service(service_url="hugs")
    data_secret = hugs.encrypt_data(data_par.secret())
    precision_secret = hugs.encrypt_data(precision_par.secret())

    auth = Authorisation(resource="process", user=authenticated_user)

    args = {
        "authorisation": auth.to_data(),
        "par": {"data": data_par.to_data(), "precision": precision_par.to_data()},
        "par_secret": {"data": data_secret, "precision": precision_secret},
        "data_type": "GCWERKS",
        "source_name": "capegrim-medusa",
        "site": "CGO",
        "instrument": "medusa",
    }

    response = hugs.call_function(function="process", args=args)

    result_keys = (sorted(response["results"]["capegrim-medusa.18.C"].keys()))[:8]

    expected_keys = [
        "capegrim-medusa.18_C4F10",
        "capegrim-medusa.18_C6F14",
        "capegrim-medusa.18_CCl4",
        "capegrim-medusa.18_CF4",
        "capegrim-medusa.18_CFC-11",
        "capegrim-medusa.18_CFC-112",
        "capegrim-medusa.18_CFC-113",
        "capegrim-medusa.18_CFC-114",
    ]

    assert result_keys == expected_keys
