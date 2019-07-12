import pytest
import os
import uuid

from HUGS.Client import Process
from Acquire.Client import User, Drive, Service, StorageCreds, PAR, Authorisation


@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("")
    return str(d)


def test_process(authenticated_user, tempdir):
    creds = StorageCreds(user=authenticated_user, service_url="storage")
    drive = Drive(creds=creds, name="test_drive")
    filepath = os.path.join(os.path.dirname(__file__), "../../../test/data/proc_test_data/CRDS/bsd.picarro.1minute.248m.dat")
    filemeta = drive.upload(filepath)

    par = PAR(location=filemeta.location(), user=authenticated_user)

    hugs = Service(service_url="hugs")
    par_secret = hugs.encrypt_data(par.secret())

    auth = Authorisation(resource="process", user=authenticated_user)

    args = {"authorisation": auth.to_data(),
            "file_par": par.to_data(),
            "par_secret": par_secret,
            "data_type": "CRDS"}

    response = hugs.call_function(function="process", args=args)

    print(response)
