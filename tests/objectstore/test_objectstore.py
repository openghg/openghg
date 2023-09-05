from typing import Any, Dict, List, Optional, TypeVar

import pytest

from openghg.store import load_metastore
from openghg.objectstore.metastore._metastore import TinyDBMetaStore
from openghg.objectstore._objectstore import AbstractDatasource, ObjectStore
from openghg.types import ObjectStoreError


@pytest.fixture
def bucket(tmp_path):
    return str(tmp_path)

@pytest.fixture
def metastore(bucket):
    """Open metastore with no data type.

    Note: `tmp_path` is function scope, so the metastore is
    reset for each test that uses this fixture.
    """
    with load_metastore(bucket=bucket, key='metastore') as session:
        metastore = TinyDBMetaStore(
            bucket=bucket,
            session=session)
        yield metastore


T = TypeVar('T', bound='InMemoryDatasource')


MetaData = Dict[str, Any]
QueryResults = List[Any]
UUID = str
Data = Any
Bucket = str


class InMemoryDatasource(AbstractDatasource):
    """Minimal class implementing the AbstractDatasource interface."""
    datasources = dict()

    def __init__(self, uuid: UUID, data: Optional[List[Data]] = None) -> None:
        self.uuid = uuid
        if data:
            self.data: List[Data] = data
        else:
            self.data: List[Data] = []

    @classmethod
    def load(cls: type[T], bucket: Bucket, uuid: UUID) -> T:
        try:
            data = cls.datasources[uuid]
        except KeyError:
            raise ValueError(f'No datasource with UUID {uuid} found in bucket {bucket}.')
        else:
            return cls(uuid, data)

    def add(self, data: Data) -> None:
        self.data.append(data)

    def delete(self) -> None:
        self.data = []
        del InMemoryDatasource.datasources[self.uuid]

    def save(self, bucket: Bucket) -> None:
        InMemoryDatasource.datasources[self.uuid] = self.data


@pytest.fixture
def objectstore(metastore, bucket):
    yield ObjectStore[InMemoryDatasource](
            metastore=metastore,
            datasource_class=InMemoryDatasource,
            bucket=bucket
        )

    # Clear datasources after test finishes
    InMemoryDatasource.datasources = dict()


@pytest.fixture
def fake_metadata():
    md1 = {'site': 'TAC', 'species': 'CH4', 'inlet': '185m'}
    md2 = {'site': 'TAC', 'species': 'CH4', 'inlet': '108m'}
    md3 = {'site': 'MHD', 'species': 'CH4', 'inlet': '10m'}
    return [md1, md2, md3]


@pytest.fixture
def fake_data():
    return list(range(100))


def test_create(objectstore, fake_metadata, fake_data):
    objectstore.create(fake_metadata[0], fake_data[0])

    assert objectstore.get_uuids()


def test_create_and_retrieve(objectstore, fake_metadata, fake_data):
    objectstore.create(fake_metadata[0], fake_data[0])
    uuid = objectstore.get_uuids()[0]
    data = objectstore.get_data(uuid).data

    assert data == [0]


def test_create_twice_raises_error(objectstore, fake_metadata, fake_data):
    objectstore.create(fake_metadata[0], fake_data[0])

    with pytest.raises(ObjectStoreError):
        objectstore.create(fake_metadata[0], fake_data[1])


def test_update(objectstore, fake_metadata, fake_data):
    objectstore.create(fake_metadata[0], fake_data[0])

    uuid = objectstore.get_uuids(fake_metadata[0])[0]
    objectstore.update(uuid, data=fake_data[1])

    data = objectstore.get_data(uuid).data

    assert len(data) == 2
    assert data == fake_data[:2]


def test_update_raises_error_if_uuid_not_found(objectstore):
    with pytest.raises(ObjectStoreError):
        objectstore.update(uuid='abc123')


def test_update_metadata(objectstore, fake_metadata, fake_data):
    objectstore.create(fake_metadata[0], fake_data[0])

    uuid = objectstore.get_uuids(fake_metadata[0])[0]

    objectstore.update(uuid, metadata={'inlet': '200m'})

    result = objectstore.search({'uuid': uuid})[0]

    assert result['inlet'] == '200m'
