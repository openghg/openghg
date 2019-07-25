import pytest
import os
import uuid

from HUGS.Client import Process
from HUGS.ObjectStore import get_local_bucket
from HUGS.Modules import GC, CRDS
from Acquire.Client import User, Drive, Service, StorageCreds, PAR, Authorisation


@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("")
    return str(d)

@pytest.fixture(autouse=True)
def run_before_tests():
    _ = get_local_bucket(empty=True)


def test_folder(filename):
    dir_path = os.path.dirname(__file__)
    test_folder = "../../../test/data/search_data"
    return os.path.join(dir_path, test_folder, filename)


def test_process_files(authenticated_user):
    service_url = "hugs"

    files = ["bsd.picarro.1minute.108m.min.dat", "hfd.picarro.1minute.100m.min.dat", "tac.picarro.1minute.100m.min.dat"]
    filepaths = [test_folder(f) for f in files]

    process = Process(service_url=service_url)

    response = process.process_files(user=authenticated_user, files=filepaths, data_type="CRDS", 
                                        hugs_url="hugs", storage_url="storage")

    print(response)

    assert False


def test_process_CRDS(authenticated_user, tempdir):
    crds = CRDS.create()
    crds.save()

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

    print(response)
    
    assert False


def test_process_GC(authenticated_user, tempdir):
    gc = GC.create()
    gc.save()

    creds = StorageCreds(user=authenticated_user, service_url="storage")
    drive = Drive(creds=creds, name="test_drive")
    data_filepath = os.path.join(os.path.dirname(__file__), "../../../test/data/proc_test_data/GC/capegrim-medusa.18.C")
    precision_filepath = os.path.join(os.path.dirname(__file__), "../../../test/data/proc_test_data/GC/capegrim-medusa.18.precisions.C")

    data_meta = drive.upload(data_filepath)
    precision_meta = drive.upload(precision_filepath)

    data_par = PAR(location=data_meta.location(), user=authenticated_user)
    precision_par = PAR(location=precision_meta.location(), user=authenticated_user)

    hugs = Service(service_url="hugs")
    data_secret = hugs.encrypt_data(data_par.secret())
    precision_secret = hugs.encrypt_data(precision_par.secret())

    auth = Authorisation(resource="process", user=authenticated_user)

    args = {"authorisation": auth.to_data(),
            "par": {"data": data_par.to_data(), "precision": precision_par.to_data()},
            "par_secret": {"data": data_secret, "precision": precision_secret},
            "data_type": "GC"}

    response = hugs.call_function(function="process", args=args)

    assert False
