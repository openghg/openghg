from typing import Any, Dict, List

import pytest
import tinydb

from openghg.objectstore.metastore._metastore import TinyDBMetaStore
from openghg.objectstore._datasource import InMemoryDatasource
from openghg.objectstore._objectstore import ObjectStore
from openghg.types import ObjectStoreError


MetaData = Dict[str, Any]
QueryResults = List[Any]
UUID = str
Data = Any
Bucket = str


@pytest.fixture
def bucket(tmp_path):
    return str(tmp_path)

@pytest.fixture
def metastore(tmp_path):
    """Open metastore with no data type.

    Note: `tmp_path` is function scope, so the metastore is
    reset for each test that uses this fixture.
    """
    metastore_path = tmp_path / 'metastore._data'
    with tinydb.TinyDB(metastore_path) as session:
        metastore = TinyDBMetaStore(session=session)
        yield metastore


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


def test_create_many(objectstore, fake_metadata, fake_data):
    """Check that creating different datasources with different metadata works."""
    objectstore.create(fake_metadata[0], fake_data[0])
    objectstore.create(fake_metadata[1], fake_data[0])
    objectstore.create(fake_metadata[2], fake_data[0])

    uuids = objectstore.get_uuids()

    assert len(uuids) == 3

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


def test_delete(objectstore, fake_metadata, fake_data):
    objectstore.create(fake_metadata[0], fake_data[0])

    uuid = objectstore.get_uuids(fake_metadata[0])[0]
    objectstore.delete(uuid)

    assert len(objectstore.get_uuids()) == 0

    with pytest.raises(LookupError):
        # LookupError from trying to load data from UUID not found in InMemoryDatasource
        objectstore.get_data(uuid)
