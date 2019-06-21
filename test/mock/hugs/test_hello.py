

import pytest
import os

from hugs.Client import Hello

@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("")
    return str(d)


def test_hello(authenticated_user, tempdir):

    hello = Hello(service_url="hugs")

    #print(hello.service().to_data())

    response = hello.send_hello("Everyone!")

    print(response)

    assert(response == "Hello Everyone!")

    response = hello.send_hello("no-one")

    assert(False)

