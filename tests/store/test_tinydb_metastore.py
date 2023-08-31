import pytest
from typing import TypeVar

from openghg.store.metastore._metastore import BucketUUIDLoadable
from openghg.store.metastore._tinydb_metastore import TinyDBMetaStore, tinydb_session
from openghg.types import MetastoreError

T = TypeVar('T', bound='DumbDatasource')

class DumbDatasource:
    """Minimal class satisfying BucketUUIDLoadable protocol."""
    def __init__(self, uuid: str) -> None:
        self.uuid = uuid

    @classmethod
    def load(cls: type[T], bucket: str, uuid: str) -> T:
        return cls(uuid)


def test_dumb_datasource_is_loadable():
    """Check that DumbDatasource satisfies BucketUUIDLoadable protocol."""
    assert issubclass(DumbDatasource, BucketUUIDLoadable)


@pytest.fixture
def metastore(tmp_path):
    """Open metastore with no data type.

    Note: `tmp_path` is function scope, so the metastore is
    reset for each test that uses this fixture.
    """
    bucket = str(tmp_path)
    with tinydb_session(bucket=bucket, data_type="") as session:
        metastore = TinyDBMetaStore[DumbDatasource](
            storage_object=DumbDatasource,
            bucket=bucket,
            session=session)
        yield metastore


@pytest.fixture
def surface_metastore(tmp_path):
    bucket = str(tmp_path)
    with tinydb_session(bucket=bucket, data_type="surface") as session:
        metastore = TinyDBMetaStore[DumbDatasource](
            storage_object=DumbDatasource,
            bucket=bucket,
            session=session)
        yield metastore


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
    """Check if we can retrieve DumbDatasource via uuid."""
    result = metastore.get(uuid="123")

    assert type(result) == DumbDatasource
    assert result.uuid == "123"


def test_search(metastore):
    """Test searching when there are multiple items
    in the metastore.
    """
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


def test_lowercase_add_search(metastore):
    """Check that keys are stored in lower case, and
    converted to lower case for searches.
    """
    metastore.add({"KEY": 1})
    metastore.add({"key": 2})

    result1 = metastore.search({"key": 1})

    assert len(result1) == 1

    result2 = metastore.search({"KEY": 2})

    assert len(result2) == 1


def test_surface_metastore(surface_metastore):
    """Check if we can use the metastore with a non-empty
    data type.
    """
    uuid = surface_metastore.add({"key1": "val1"})
    result = surface_metastore.search()

    assert len(result) == 1

    assert result[0]['uuid'] == uuid
    assert result[0]['key1'] == 'val1'


def test_multiple_metastores(metastore, surface_metastore):
    """Check that metastores with different data types do not
    interact.
    """
    metastore.add({"key": 1})
    surface_metastore.add({"key": 1})

    res1 = metastore.search()

    assert len(res1) == 1

    res2 = surface_metastore.search()

    assert len(res2) == 1


def test_read_only_metastore(tmp_path):
    """Check that adding to a metastore opened with
    a read-only TinyDB raises a MetaStoreError
    """
    bucket = str(tmp_path)
    with pytest.raises(MetastoreError):
        with tinydb_session(bucket=bucket, data_type='', mode='r') as session:
            read_only_metastore = TinyDBMetaStore[DumbDatasource](
                storage_object=DumbDatasource,
                bucket=bucket,
                session=session,
            )
            read_only_metastore.add({"key": "val"})
