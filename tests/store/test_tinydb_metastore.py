import pytest
from typing import TypeVar

from openghg.store.metastore._metastore import BucketUUIDLoadable
from openghg.store.metastore._tinydb_metastore import TinyDBMetaStore

T = TypeVar('T', bound='DumbDatasource')

class DumbDatasource:
    def __init__(self, bucket: str, uuid: str) -> None:
        self.bucket = bucket
        self.uuid = uuid

    @classmethod
    def load(cls: type[T], bucket: str, uuid: str) -> T:
        return cls(bucket, uuid)


def test_dumb_datasource_is_loadable():
    """Check that DumbDatasource satisfies BucketUUIDLoadable protocol."""
    assert issubclass(DumbDatasource, BucketUUIDLoadable)


@pytest.fixture()
def metastore(tmp_path):
    metastore = TinyDBMetaStore[DumbDatasource](storage_object=DumbDatasource, bucket=str(tmp_path), data_type="")
    yield metastore
    metastore._metastore.close()


def test_init(metastore):
    assert metastore.data_type == ""
    assert metastore._storage_object == DumbDatasource


def test_search_empty(metastore):
    results = metastore.search()
    assert results == []


def test_add(metastore):
    uuid = metastore.add({"key1": "val1"})
    result = metastore.search()

    assert len(result) == 1

    assert result[0]['uuid'] == uuid
    assert result[0]['key1'] == 'val1'


def test_get(metastore):
    result = metastore.get(uuid="123")

    assert result.uuid == "123"


def test_search(metastore):
    uuid1 = metastore.add({"key": "val1"})
    uuid2 = metastore.add({"key": "val2"})

    result_all = metastore.search()

    assert len(result_all) == 2

    result1 = metastore.search({'key': 'val1'})

    assert result1[0]['uuid'] == uuid1
    assert result1[0]['key'] == 'val1'

    result2 = metastore.search({'key': 'val2'})

    assert result2[0]['uuid'] == uuid2
    assert result2[0]['key'] == 'val2'
