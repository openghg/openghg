from pathlib import Path

import pytest
from openghg.objectstore import get_local_objectstore_path, get_tutorial_store_path

# @pytest.fixture(scope="session")
# def setup_config():
@pytest.fixture()
def mock_read_config(mocker):
    mock_config = {"object_store": {"local_store": "/tmp/example_store"}}
    mocker.patch("toml.loads", return_value=mock_config)


def test_get_local_object_store(mock_read_config):
    path = get_local_objectstore_path()
    assert path == Path("/tmp/example_store")

def test_get_tutorial_store_path(mock_read_config):
    path = get_tutorial_store_path()
    assert path == Path("/tmp/example_store/tutorial_store")


# def test_get_tutorial_store(mocker):

#     mocker.patch("toml.load", )


# @pytest.mark.skip(reason="Unfinished")
# def test_query_store(populate_store):
#     data = query_store()

# print(data)
