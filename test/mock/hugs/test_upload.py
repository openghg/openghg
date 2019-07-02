import pytest
import os

from Acquire.Client import Drive, StorageCreds

from HUGS.Client import ListObjects

@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("")
    return str(d)

def test_upload(authenticated_user, tempdir):
    filename = __file__
    drive_name = "test_drive"
    creds = StorageCreds(user=authenticated_user, service_url="storage")
    drive = Drive(name=drive_name, creds=creds, autocreate=True)
    filemeta = drive.upload(filename=filename)

    print(filemeta.to_data())

    assert(False)
