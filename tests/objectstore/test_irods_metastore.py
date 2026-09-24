"""Catalog behavior using real client AVU/query types and no iRODS server."""

import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

pytest.importorskip("irods")
from irods.exception import DataObjectDoesNotExist
from irods.meta import iRODSMeta
from irods.models import Collection, DataObject, DataObjectMeta

from openghg.objectstore._irods_metastore import (
    FIELD_PREFIX,
    MAX_AVU_BYTES,
    RECORD_ATTRIBUTE,
    IRODSMetaStore,
    encode_metadata,
)
from openghg.types import MetastoreError, ObjectStoreError


class CatalogMetadata:
    def __init__(self):
        self.avus = [iRODSMeta("unrelated", "keep me")]
        self.calls = 0
        self.fail = False

    def get_all(self, name):
        return [avu for avu in self.avus if avu.name == name]

    def items(self):
        return list(self.avus)

    def apply_atomic_operations(self, *operations):
        self.calls += 1
        updated = list(self.avus)
        for operation in operations:
            if operation.operation == "remove":
                updated.remove(operation.avu)
            else:
                updated.append(operation.avu)
        if self.fail:
            raise RuntimeError("catalog rejected atomic metadata update")
        self.avus = updated


class CatalogQuery:
    def __init__(self, session):
        self.session = session
        self.criteria = []
        self.keywords = {}

    def filter(self, *criteria):
        self.criteria.extend(criteria)
        return self

    def add_keyword(self, name, value):
        self.keywords[name] = value
        return self

    def __iter__(self):
        for path, obj in self.session.objects.items():
            collection, name = path.rsplit("/", 1)
            for avu in obj.metadata.items():
                values = {
                    Collection.name: collection,
                    DataObjectMeta.name: avu.name,
                    DataObjectMeta.value: avu.value,
                }
                assert all(criterion.op == "=" for criterion in self.criteria)
                if all(values[criterion.query_key] == criterion.value for criterion in self.criteria):
                    yield {DataObject.name: name}


class CatalogSession:
    def __init__(self):
        self.objects = {}
        self.data_objects = SimpleNamespace(get=self.get)
        self.queries = []

    def get(self, path):
        try:
            return self.objects[path]
        except KeyError:
            raise DataObjectDoesNotExist(path)

    def query(self, *columns):
        assert columns == (DataObject.name,)
        query = CatalogQuery(self)
        self.queries.append(query)
        return query


@pytest.fixture
def metastore():
    return IRODSMetaStore(CatalogSession(), "/testZone/home/alice/snapshots", "rw")


def upload(metastore, metadata):
    """Simulate an existing payload, then publish its catalog record."""
    uuid = str(uuid4())
    metastore.session.objects[metastore.path(uuid)] = SimpleNamespace(metadata=CatalogMetadata())
    metastore.insert({"uuid": uuid, **metadata})
    return uuid


def test_publish_search_scope_and_typed_metadata(metastore):
    uuid = upload(metastore, {"Site": "TAC", "levels": [1, 2], "info": {"Flag": True}})
    nested = IRODSMetaStore(metastore.session, metastore.collection + "/nested", "rw")
    upload(nested, {"site": "TAC"})
    unpublished = str(uuid4())
    metastore.session.objects[metastore.path(unpublished)] = SimpleNamespace(metadata=CatalogMetadata())

    expected = {"uuid": uuid, "site": "TAC", "levels": [1, 2], "info": {"Flag": True}}
    assert metastore.search({"SITE": "TAC"}) == [expected]
    assert metastore.search() == [expected]
    assert metastore.session.queries[0].keywords == {"zone": "testZone"}
    assert any(
        c.query_key is DataObjectMeta.name and c.value == FIELD_PREFIX + "site"
        for c in metastore.session.queries[0].criteria
    )
    assert metastore.session.get(metastore.path(uuid)).metadata.get_all("unrelated")[0].value == "keep me"
    with pytest.raises(ObjectStoreError, match="published"):
        metastore.record(unpublished)


def test_search_preserves_predicates_lists_and_numeric_equality(metastore):
    uuid = upload(metastore, {"site": "TAC", "level": 1, "groups": ["user", "admin"]})
    upload(metastore, {"site": "MHD", "level": 2, "groups": ["user"], "extra": "present"})
    assert [r["uuid"] for r in metastore.search({"level": 1.0})] == [uuid]
    assert [r["uuid"] for r in metastore.search({"level": True})] == [uuid]
    assert [
        r["uuid"]
        for r in metastore.search(
            search_functions={"level": lambda value: value < 2},
            search_list_keys={"groups": "admin"},
            negative_lookup_keys=["extra"],
        )
    ] == [uuid]


@pytest.mark.parametrize(
    "key,value", [("owner", "O'Brien"), ("path", "a\\b"), ("label", 'a"b'), ("owner's", "name")]
)
def test_search_scans_marker_when_genquery_cannot_escape_term(metastore, key, value):
    uuid = upload(metastore, {key: value})
    assert [record["uuid"] for record in metastore.search({key: value})] == [uuid]
    assert any(
        criterion.query_key is DataObjectMeta.name and criterion.value == RECORD_ATTRIBUTE
        for criterion in metastore.session.queries[-1].criteria
    )


def test_atomic_update_and_delete_preserve_foreign_avus_and_payload(metastore):
    uuid = upload(metastore, {"site": "TAC", "groups": ["user"], "old": "value"})
    obj = metastore.session.get(metastore.path(uuid))
    metastore.update({"uuid": uuid}, {"site": "MHD"}, "old", {"groups": "admin"})
    assert metastore.record(uuid) == {"uuid": uuid, "site": "MHD", "groups": ["user", "admin"]}
    assert obj.metadata.calls == 2
    assert not metastore.search({"site": "TAC"})
    assert metastore.search({"site": "MHD"})
    assert obj.metadata.get_all("unrelated")

    before = obj.metadata.items()
    obj.metadata.fail = True
    with pytest.raises(RuntimeError, match="catalog rejected"):
        metastore.update({"uuid": uuid}, {"site": "failed"})
    assert obj.metadata.items() == before

    obj.metadata.fail = False
    metastore.delete({"uuid": uuid})
    assert metastore.session.get(metastore.path(uuid)) is obj
    assert obj.metadata.items() == [iRODSMeta("unrelated", "keep me")]
    assert not metastore.search()


def test_mutation_requires_one_match_and_preserves_uuid(metastore):
    uuid = upload(metastore, {"site": "TAC"})
    upload(metastore, {"site": "TAC"})
    for where in ({"site": "TAC"}, {"site": "absent"}):
        with pytest.raises(MetastoreError):
            metastore.update(where, {"site": "other"})
        with pytest.raises(MetastoreError):
            metastore.delete(where)
    with pytest.raises(ValueError, match="UUID"):
        metastore.update({"uuid": uuid}, to_delete="uuid")
    with pytest.raises(ValueError, match="UUID"):
        metastore.update({"uuid": uuid}, {"uuid": str(uuid4())})
    with pytest.raises(MetastoreError, match="already published"):
        metastore.insert({"uuid": uuid})
    metastore.delete({"site": "TAC"}, delete_one=False)
    assert not metastore.search()


def test_read_only_rejects_all_mutation_before_catalog_access(metastore):
    readonly = IRODSMetaStore(metastore.session, metastore.collection)
    for mutate in (lambda: readonly.insert({}), lambda: readonly.update({}), lambda: readonly.delete({})):
        with pytest.raises(PermissionError):
            mutate()
    readonly.close()
    assert not metastore.session.queries


@pytest.mark.parametrize(
    "uuid",
    [
        "../escape",
        "/absolute",
        "invalid",
        "12345678-1234-1234-1234-ABCDEF123456",
        "123456781234123412341234567890ab",
    ],
)
def test_path_rejects_unsafe_or_noncanonical_identifiers(metastore, uuid):
    with pytest.raises(ValueError):
        metastore.path(uuid)


def test_record_rejects_malformed_or_ambiguous_publication(metastore):
    uuid = upload(metastore, {"site": "TAC"})
    avus = metastore.session.get(metastore.path(uuid)).metadata
    for value in (
        "not json",
        "[]",
        json.dumps({"uuid": str(uuid4())}),
        json.dumps({"uuid": uuid, "bad": float("nan")}),
    ):
        avus.avus = [iRODSMeta(RECORD_ATTRIBUTE, value)]
        with pytest.raises(MetastoreError, match="Invalid"):
            metastore.record(uuid)
    avus.avus = [iRODSMeta(RECORD_ATTRIBUTE, "{}"), iRODSMeta(RECORD_ATTRIBUTE, "{}")]
    with pytest.raises(MetastoreError, match="Multiple"):
        metastore.record(uuid)


@pytest.mark.parametrize(
    "metadata,exception",
    [
        ({"Site": 1, "site": 2}, ValueError),
        ({"value": float("inf")}, ValueError),
        ({"nested": {1: "value"}}, TypeError),
        ({"value": object()}, TypeError),
    ],
)
def test_encoder_rejects_values_that_cannot_roundtrip(metadata, exception):
    with pytest.raises(exception):
        encode_metadata(metadata)


def test_encoder_enforces_utf8_avu_boundaries():
    overhead = len(encode_metadata({"value": ""}).encode("utf-8"))
    assert len(encode_metadata({"value": "x" * (MAX_AVU_BYTES - overhead)}).encode("utf-8")) == MAX_AVU_BYTES
    with pytest.raises(ValueError, match="records"):
        encode_metadata({"value": "x" * (MAX_AVU_BYTES - overhead + 1)})
    with pytest.raises(ValueError, match="records"):
        encode_metadata({"value": "é" * (MAX_AVU_BYTES // 2)})
    with pytest.raises(ValueError, match="names"):
        encode_metadata({"x" * (MAX_AVU_BYTES - len(FIELD_PREFIX) + 1): 0})
    with pytest.raises(ValueError, match="control"):
        encode_metadata({"bad\x00key": "value"})
