"""Unit tests for the ObjectStore class."""
from typing import Any, ClassVar, TypeVar
from typing_extensions import Self

import pytest
import tinydb

from openghg.objectstore.metastore._metastore import TinyDBMetaStore
from openghg.objectstore._datasource import AbstractDatasource, DatasourceFactory
from openghg.objectstore._objectstore import ObjectStore
from openghg.types import ObjectStoreError


MetaData = dict[str, Any]
QueryResults = list[Any]
UUID = str
Data = TypeVar("Data")


@pytest.fixture
def metastore(tmp_path):
    """Open metastore with no data type.

    Note: `tmp_path` is function scope, so the metastore is
    reset for each test that uses this fixture.
    """
    metastore_path = tmp_path / "metastore._data"
    with tinydb.TinyDB(metastore_path) as session:
        metastore = TinyDBMetaStore(database=session)
        yield metastore


class InMemoryDatasource(AbstractDatasource):
    """Minimal class implementing the Datasource interface.

    The data stored by each instance of `InMemoryDatasource` is just a list.

    There is a class variable `datasources`, which is a dict mapping UUIDs to the list
    of data for the datasource with that UUID. This allows us to simulate loading data.

    When data is added to InMemoryDatasource, it is just appended to that
    datasource's list.
    """

    datasources: ClassVar[dict[UUID, list[Any]]] = {}

    def __init__(self, uuid: UUID, data: list[Any] | None = None, **kwargs: Any) -> None:
        super().__init__(uuid)
        if data:
            self.data: list[Any] = data
        else:
            self.data: list[Any] = []

    @classmethod
    def load(cls: type[Self], uuid: UUID) -> Self:
        try:
            data = cls.datasources[uuid]
        except KeyError:
            raise LookupError(f"No datasource with UUID {uuid} found.")
        else:
            return cls(uuid, data)

    def add(self, data: Any) -> None:
        self.data.append(data)

    def get_data(self) -> Any:
        return self.data

    def delete(self) -> None:
        self.data = []
        del InMemoryDatasource.datasources[self.uuid]

    def save(self) -> None:
        InMemoryDatasource.datasources[self.uuid] = self.data


@pytest.fixture
def objectstore(metastore):
    """Create ObjectStore with simple fake Datasource.

    InMemoryDatasource is used for unit testing ObjectStore
    without needed to setup or mock a "real" Datasoure.
    """
    yield ObjectStore[InMemoryDatasource, Any](
        metastore, DatasourceFactory[InMemoryDatasource](InMemoryDatasource)
    )

    # Clear datasources after test finishes
    InMemoryDatasource.datasources = {}


@pytest.fixture
def fake_metadata():
    md1 = {"site": "TAC", "species": "CH4", "inlet": "185m"}
    md2 = {"site": "TAC", "species": "CH4", "inlet": "108m"}
    md3 = {"site": "MHD", "species": "CH4", "inlet": "10m"}
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
    data = objectstore.get_datasource(uuid).get_data()

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
    """Test that we can update an existing Datasource."""
    # create a datasource
    objectstore.create(fake_metadata[0], fake_data[0])

    # update the data by uuid
    uuid = objectstore.get_uuids(fake_metadata[0])[0]
    objectstore.update(uuid, data=fake_data[1])

    # check that the new data has been appended (which is what we expect from InMemoryDatasource)
    data = objectstore.get_datasource(uuid).get_data()

    assert len(data) == 2
    assert data == fake_data[:2]


def test_update_raises_error_if_uuid_not_found(objectstore):
    with pytest.raises(ObjectStoreError):
        objectstore.update(uuid="abc123")


def test_update_metadata(objectstore, fake_metadata, fake_data):
    """Test that we can modify the metadata stored in the metastore.

    NOTE: eventually, object store might return Datasource (or something like it),
    in which case we would need to change the assertions in this test.
    """
    objectstore.create(fake_metadata[0], fake_data[0])

    uuid = objectstore.get_uuids(fake_metadata[0])[0]

    objectstore.update(uuid, metadata={"inlet": "200m"})

    result = objectstore.search({"uuid": uuid})[0]

    assert result["inlet"] == "200m"


def test_delete(objectstore, fake_metadata, fake_data):
    """Check that deleting an entry in the object store clears metadata and datasource.

    Note that we need to check that the uuid is not in the metastore and that the datasource
    has been deleted separately, since these two actions are independent.
    """
    objectstore.create(fake_metadata[0], fake_data[0])

    uuid = objectstore.get_uuids(fake_metadata[0])[0]
    objectstore.delete(uuid)

    # check that uuid is not in the object store (which means that it is not in the metastore)
    assert uuid not in objectstore.uuids

    # check that we can't load the datasource
    with pytest.raises(LookupError):
        # LookupError from trying to load data from UUID not found in InMemoryDatasource
        objectstore.get_datasource(uuid)
