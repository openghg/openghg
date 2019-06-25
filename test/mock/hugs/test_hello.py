

import pytest
import os

from Acquire.Client import Drive, StorageCreds

from HUGS.Client import Hello

@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("")
    return str(d)


def test_hello(authenticated_user, tempdir):

    print(tempdir)

    drive_name = "hugs_files"

    creds = StorageCreds(user=authenticated_user, service_url="storage")

    drive = Drive(name=drive_name, creds=creds)

    filemeta = drive.upload(filename=__file__, uploaded_name="test.py")

    print(filemeta.__dict__)

    location = filemeta.location()

    print(location.to_string())

    hello = Hello(service_url="hugs")

    #print(hello.service().to_data())

    response = hello.send_hello("Everyone!")

    print(response)

    assert(response == "Hello Everyone!")

    response = hello.send_hello("no-one")

    assert(False)

