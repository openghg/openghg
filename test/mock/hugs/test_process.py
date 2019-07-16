import pytest
import os
import uuid

from HUGS.Client import Process
from Acquire.Client import User, Drive, Service, StorageCreds, PAR, Authorisation


@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("")
    return str(d)


def test_process_CRDS(authenticated_user, tempdir):
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

    print(response)

    assert False
