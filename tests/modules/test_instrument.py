import datetime
import uuid
import pytest

from Acquire.ObjectStore import ObjectStore
from Acquire.ObjectStore import string_to_encoded
# from objectstore.hugs_objstore import get_bucket

from modules import _datasource as Datasource
from objectstore import local_bucket

mocked_uuid = "00000000-0000-0000-00000-000000000000"

@pytest.fixture
def mock_uuid(monkeypatch):

    def mock_uuid():
        return mocked_uuid

    monkeypatch.setattr(uuid, 'uuid4', mock_uuid)


def test_create(mock_uuid):
    
