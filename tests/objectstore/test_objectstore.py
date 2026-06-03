"""Unit tests for the ObjectStore class."""

from typing import Any, ClassVar, TypeVar
from typing_extensions import Self

import pytest
import tinydb

from openghg.objectstore.metastore._metastore import TinyDBMetaStore
from openghg.objectstore._datasource import AbstractDatasource, DatasourceFactory
from openghg.objectstore._objectstore import ObjectStore, make_metadata_updater_fn
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
    datasource_metadata: ClassVar[dict[UUID, MetaData]] = {}

    def __init__(
        self,
        uuid: UUID,
        data: list[Any] | None = None,
        metadata: MetaData | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(uuid)
        if data:
            self.data: list[Any] = data
        else:
            self.data: list[Any] = []
        self.metadata: MetaData = dict(metadata or {})

    @classmethod
    def load(cls: type[Self], uuid: UUID) -> Self:
        try:
            data = cls.datasources[uuid]
        except KeyError:
            raise LookupError(f"No datasource with UUID {uuid} found.")
        else:
            return cls(uuid, data, metadata=cls.datasource_metadata.get(uuid, {}))

    def add(self, data: Any) -> None:
        self.data.append(data)

    def get_data(self) -> Any:
        return self.data

    def delete(self) -> None:
        self.data = []
        del InMemoryDatasource.datasources[self.uuid]
        InMemoryDatasource.datasource_metadata.pop(self.uuid, None)

    def save(self) -> None:
        InMemoryDatasource.datasources[self.uuid] = self.data
        InMemoryDatasource.datasource_metadata[self.uuid] = dict(self.metadata)


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
    InMemoryDatasource.datasource_metadata = {}


@pytest.fixture
def merged_objectstore(metastore):
    """Create ObjectStore that merges in-memory Datasource metadata."""
    yield ObjectStore[InMemoryDatasource, Any](
        metastore,
        DatasourceFactory[InMemoryDatasource](InMemoryDatasource),
        metadata_updater=make_metadata_updater_fn(extend_keys=["groups"]),
    )

    InMemoryDatasource.datasources = {}
    InMemoryDatasource.datasource_metadata = {}


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


def test_search_uses_datasource_only_metadata_when_metastore_index_lacks_key(merged_objectstore, fake_data):
    uuid = merged_objectstore.create(
        {"site": "TAC", "species": "CH4"},
        fake_data[0],
    )
    InMemoryDatasource.datasource_metadata[uuid] = {"source": "displayed-source"}

    assert merged_objectstore.metastore.search({"source": "displayed-source"}) == []

    results = merged_objectstore.search(site="TAC", source="displayed-source")
    retrieved = merged_objectstore.retrieve(site="TAC", source="displayed-source")

    assert [result["uuid"] for result in results] == [uuid]
    assert results[0]["source"] == "displayed-source"
    assert len(retrieved) == 1
    assert retrieved[0].uuid == uuid


def test_search_returns_raw_and_datasource_only_metadata_matches(merged_objectstore, fake_data):
    raw_match_uuid = merged_objectstore.create(
        {"site": "TAC", "species": "CH4", "source": "displayed-source"},
        fake_data[0],
    )
    datasource_match_uuid = merged_objectstore.create(
        {"site": "MHD", "species": "CH4"},
        fake_data[1],
    )
    InMemoryDatasource.datasource_metadata[datasource_match_uuid] = {"source": "displayed-source"}

    results = merged_objectstore.search(source="displayed-source")
    retrieved = merged_objectstore.retrieve(source="displayed-source")

    assert {result["uuid"] for result in results} == {raw_match_uuid, datasource_match_uuid}
    assert {datasource.uuid for datasource in retrieved} == {raw_match_uuid, datasource_match_uuid}


def test_search_skips_unrelated_missing_datasource_candidate(merged_objectstore, fake_data):
    uuid = merged_objectstore.create(
        {"site": "TAC", "species": "CH4"},
        fake_data[0],
    )
    InMemoryDatasource.datasource_metadata[uuid] = {"source": "displayed-source"}
    merged_objectstore.metastore.insert(
        {"uuid": "missing-datasource", "site": "MHD", "species": "CH4", "source": "displayed-source"}
    )

    results = merged_objectstore.search(site="TAC", species="CH4", source="displayed-source")
    retrieved = merged_objectstore.retrieve(site="TAC", species="CH4", source="displayed-source")
    uuids = merged_objectstore.get_uuids({"site": "TAC", "species": "CH4", "source": "displayed-source"})

    assert [result["uuid"] for result in results] == [uuid]
    assert [datasource.uuid for datasource in retrieved] == [uuid]
    assert uuids == [uuid]


def test_merged_metadata_view_applies_function_negative_and_list_searches(merged_objectstore, fake_data):
    matching_uuid = merged_objectstore.create({"site": "TAC", "species": "CH4"}, fake_data[0])
    other_uuid = merged_objectstore.create({"site": "MHD", "species": "CH4"}, fake_data[1])
    InMemoryDatasource.datasource_metadata[matching_uuid] = {
        "inlet": 200,
        "groups": ["user", "admin"],
        "extra_key": "present",
    }
    InMemoryDatasource.datasource_metadata[other_uuid] = {
        "inlet": 50,
        "groups": ["user"],
    }

    def inlet_over_100(value):
        return value > 100

    results = merged_objectstore.search(
        search_functions={"inlet": inlet_over_100},
        search_list_keys={"groups": "admin"},
    )
    negative_results = merged_objectstore.search(negative_lookup_keys=["extra_key"])

    assert [result["uuid"] for result in results] == [matching_uuid]
    assert [result["uuid"] for result in negative_results] == [other_uuid]


def test_merged_metadata_view_select_uses_raw_value_for_duplicate_keys(merged_objectstore, fake_data):
    uuid = merged_objectstore.create({"site": "TAC", "source": "raw-source"}, fake_data[0])
    InMemoryDatasource.datasource_metadata[uuid] = {"source": "displayed-source"}

    assert merged_objectstore._metadata_view.select("source") == ["raw-source"]


def test_datasource_managed_metadata_overrides_raw_metastore_metadata(merged_objectstore, fake_data):
    uuid = merged_objectstore.create(
        {
            "site": "TAC",
            "source": "raw-source",
            "sampling_period": "12H",
            "start_date": "2020-01-01 00:00:00+00:00",
            "tag": "ceda_v1",
        },
        fake_data[0],
    )
    InMemoryDatasource.datasource_metadata[uuid] = {
        "sampling_period": "12h",
        "source": "datasource-source",
        "start_date": "2019-01-01 00:00:00+00:00",
        "tag": ["ceda_v1"],
    }

    result = merged_objectstore.search(site="TAC")[0]

    assert result["sampling_period"] == "12h"
    assert result["source"] == "raw-source"
    assert result["start_date"] == "2019-01-01 00:00:00+00:00"
    assert result["tag"] == ["ceda_v1"]


def test_search_does_not_mutate_datasource_metadata(merged_objectstore, fake_data):
    uuid = merged_objectstore.create(
        {"site": "TAC", "species": "CH4", "source": "raw-source"},
        fake_data[0],
    )
    datasource_metadata = {"source": "displayed-source"}
    InMemoryDatasource.datasource_metadata[uuid] = dict(datasource_metadata)

    merged_objectstore.search(site="TAC", source="raw-source")
    datasource = merged_objectstore.get_datasource(uuid)

    assert datasource.metadata == datasource_metadata


def test_retrieve_returns_datasource_with_merged_metadata_without_saving(merged_objectstore, fake_data):
    uuid = merged_objectstore.create(
        {"site": "TAC", "species": "CH4", "source": "raw-source"},
        fake_data[0],
    )
    datasource_metadata = {"source": "displayed-source"}
    InMemoryDatasource.datasource_metadata[uuid] = dict(datasource_metadata)

    datasource = merged_objectstore.retrieve(site="TAC", source="raw-source")[0]
    reloaded_datasource = merged_objectstore.get_datasource(uuid)

    assert datasource.metadata["site"] == "tac"
    assert datasource.metadata["species"] == "ch4"
    assert datasource.metadata["source"] == "raw-source"
    assert reloaded_datasource.metadata == datasource_metadata


def test_create_rejects_duplicate_from_datasource_only_metadata(merged_objectstore, fake_data):
    uuid = merged_objectstore.create({"site": "TAC"}, fake_data[0])
    InMemoryDatasource.datasource_metadata[uuid] = {"source": "displayed-source"}

    with pytest.raises(ObjectStoreError):
        merged_objectstore.create({"site": "TAC", "source": "displayed-source"}, fake_data[1])


def test_create_rejects_duplicate_from_raw_metadata_when_datasource_has_duplicate_key(
    merged_objectstore, fake_data
):
    uuid = merged_objectstore.create({"site": "TAC", "source": "raw-source"}, fake_data[0])
    InMemoryDatasource.datasource_metadata[uuid] = {"source": "displayed-source"}

    with pytest.raises(ObjectStoreError):
        merged_objectstore.create({"site": "TAC", "source": "raw-source"}, fake_data[1])


def test_uuids_uses_raw_metastore_when_unfiltered(merged_objectstore):
    merged_objectstore.metastore.insert({"uuid": "missing-datasource", "site": "TAC"})

    assert merged_objectstore.uuids == ["missing-datasource"]
