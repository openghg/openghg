"""Catalog metastore contracts without a running iRODS server."""

from contextlib import nullcontext
from copy import deepcopy
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from openghg.objectstore import _irods_metastore
from openghg.objectstore._irods_metastore import IRODSMetaStore, encode_metadata
from openghg.types import MetastoreError, ObjectStoreError


@pytest.fixture
def catalog(monkeypatch):
    documents, collections, reads = {}, set(), []
    state = SimpleNamespace(documents=documents, collections=collections, reads=reads, fail=False)

    def create(path, recurse):
        assert recurse
        collections.add(path)

    session = SimpleNamespace(collections=SimpleNamespace(create=create))

    def factory():
        return nullcontext(session)

    def read(session_factory, collection, key):
        assert session_factory is factory
        reads.append((collection, key))
        value = documents[(collection, key)]
        if isinstance(value, Exception):
            raise value
        return deepcopy(value)

    def write(session_factory, collection, key, value):
        assert session_factory is factory
        assert collection in collections
        if state.fail:
            raise RuntimeError("catalog rejected atomic metadata update")
        documents[(collection, key)] = deepcopy(value)

    def children(session_factory, collection):
        assert session_factory is factory
        return sorted(path for path in collections if path.rsplit("/", 1)[0] == collection)

    monkeypatch.setattr(_irods_metastore, "read_document", read)
    monkeypatch.setattr(_irods_metastore, "write_document", write)
    monkeypatch.setattr(_irods_metastore, "list_collections", children)
    state.factory = factory
    state.metastore = IRODSMetaStore(factory, "/testZone/home/alice/openghg/surface", "rw")
    return state


def publish(metastore, metadata):
    uuid = str(uuid4())
    metastore.publish(uuid, {"uuid": uuid, **metadata}, {"_uuid": uuid, "_data_keys": {}}, {}, None)
    return uuid


def test_published_scope_typed_metadata_and_no_payload_access(catalog):
    metastore = catalog.metastore
    uuid = publish(metastore, {"Site": "TAC", "levels": [1, 2], "info": {"Flag": True}})
    nested = IRODSMetaStore(catalog.factory, metastore.collection + "/nested", "rw")
    publish(nested, {"site": "TAC"})
    unpublished = str(uuid4())
    catalog.collections.add(metastore.path(unpublished))
    catalog.documents[(metastore.path(unpublished), "datasource")] = {"_uuid": unpublished}
    catalog.collections.add(metastore.collection + "/not-a-uuid")

    expected = {"uuid": uuid, "site": "TAC", "levels": [1, 2], "info": {"Flag": True}}
    assert metastore.search({"SITE": "TAC"}) == [expected]
    assert metastore.search() == [expected]
    assert all(key in {"publication", "record", "datasource"} for _, key in catalog.reads)
    with pytest.raises(ObjectStoreError, match="published"):
        metastore.record(unpublished)


def test_search_preserves_predicates_lists_and_numeric_equality(catalog):
    metastore = catalog.metastore
    uuid = publish(metastore, {"site": "TAC", "level": 1, "groups": ["user", "admin"]})
    publish(metastore, {"site": "MHD", "level": 2, "groups": ["user"], "extra": "present"})
    assert [r["uuid"] for r in metastore.search({"level": 1.0})] == [uuid]
    assert [r["uuid"] for r in metastore.search({"level": True})] == [uuid]
    assert [
        r["uuid"]
        for r in metastore.search(
            search_functions={"LEVEL": lambda value: value < 2},
            search_list_keys={"groups": "admin"},
            negative_lookup_keys=["EXTRA"],
        )
    ] == [uuid]


@pytest.mark.parametrize(
    "key,value", [("owner", "O'Brien"), ("path", "a\\b"), ("label", 'a"b'), ("owner's", "name")]
)
def test_quoted_metadata_does_not_become_catalog_query_syntax(catalog, key, value):
    uuid = publish(catalog.metastore, {key: value})
    assert [record["uuid"] for record in catalog.metastore.search({key: value})] == [uuid]


def test_update_delete_preserve_datasource_state_and_payload_collection(catalog):
    metastore = catalog.metastore
    uuid = publish(metastore, {"site": "TAC", "groups": ["user"], "old": "value"})
    path = metastore.path(uuid)
    catalog.documents[(path, "datasource")] = {"_uuid": uuid, "_latest_version": "v1"}
    catalog.collections.add(path + "/v1")

    metastore.update({"uuid": uuid}, {"site": "MHD"}, "old", {"groups": "admin"})
    expected = {"uuid": uuid, "site": "MHD", "groups": ["user", "admin"]}
    assert metastore.record(uuid) == expected
    assert not metastore.search({"site": "TAC"})
    assert metastore.search({"site": "MHD"})

    catalog.fail = True
    with pytest.raises(RuntimeError, match="catalog rejected"):
        metastore.update({"uuid": uuid}, {"site": "failed"})
    assert metastore.record(uuid) == expected
    catalog.fail = False

    metastore.delete({"uuid": uuid})
    assert path in catalog.collections
    assert path + "/v1" in catalog.collections
    assert catalog.documents[(path, "datasource")] == {"_uuid": uuid, "_latest_version": "v1"}
    assert not metastore.search()


def test_mutation_requires_one_match_and_preserves_uuid(catalog):
    metastore = catalog.metastore
    uuid = publish(metastore, {"site": "TAC"})
    publish(metastore, {"site": "TAC"})
    for where in ({"site": "TAC"}, {"site": "absent"}):
        with pytest.raises(MetastoreError):
            metastore.update(where, {"site": "other"})
        with pytest.raises(MetastoreError):
            metastore.delete(where)
    for change in (
        {"to_delete": "uuid"},
        {"to_update": {"uuid": str(uuid4())}},
        {"to_extend": {"uuid": "other"}},
    ):
        with pytest.raises(ValueError, match="UUID"):
            metastore.update({"uuid": uuid}, **change)
    with pytest.raises(MetastoreError, match="already published"):
        metastore.insert({"uuid": uuid})
    metastore.delete({"site": "TAC"}, delete_one=False)
    assert not metastore.search()


def test_large_metadata_roundtrip_update_and_validation(catalog):
    metastore = catalog.metastore
    metadata = {"description": "é" * 8000, "versions": {f"v{i}": ["start_end"] for i in range(150)}}
    assert len(encode_metadata(metadata).encode("utf-8")) > 2700
    uuid = publish(metastore, metadata)
    assert metastore.record(uuid) == {"uuid": uuid, **metadata}
    metastore.update(
        {"uuid": uuid}, to_update={"versions": {**metadata["versions"], "v150": ["next_start_end"]}}
    )
    assert metastore.record(uuid)["versions"]["v150"] == ["next_start_end"]
    assert metastore.search({"description": metadata["description"]})
    before = metastore.record(uuid)
    with pytest.raises(ValueError):
        metastore.update({"uuid": uuid}, {"bad": float("nan")})
    assert metastore.record(uuid) == before


def test_read_only_rejects_all_mutation_before_catalog_access(catalog):
    readonly = IRODSMetaStore(catalog.factory, catalog.metastore.collection)
    for mutate in (lambda: readonly.insert({}), lambda: readonly.update({}), lambda: readonly.delete({})):
        with pytest.raises(PermissionError):
            mutate()
    readonly.close()
    assert not catalog.reads
    assert not catalog.collections
    assert readonly.search() == []


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
def test_path_rejects_unsafe_or_noncanonical_identifiers(catalog, uuid):
    with pytest.raises(ValueError):
        catalog.metastore.path(uuid)


@pytest.mark.parametrize(
    "corruption", [[], {"uuid": "other"}, {"bad": float("nan")}, ObjectStoreError("broken document")]
)
def test_record_and_search_reject_corruption_instead_of_hiding_it(catalog, corruption):
    metastore = catalog.metastore
    uuid = publish(metastore, {"site": "TAC"})
    if isinstance(corruption, dict) and "uuid" not in corruption:
        corruption["uuid"] = uuid
    if isinstance(corruption, Exception):
        catalog.documents[(metastore.path(uuid), "publication")] = corruption
    else:
        catalog.documents[(metastore.path(uuid), "publication")]["record"] = corruption
    with pytest.raises(MetastoreError, match="Invalid"):
        metastore.record(uuid)
    with pytest.raises(MetastoreError, match="Invalid"):
        metastore.search()


@pytest.mark.parametrize(
    "metadata,exception",
    [
        ({"Site": 1, "site": 2}, ValueError),
        ({"value": float("inf")}, ValueError),
        ({"nested": {1: "value"}}, TypeError),
        ({"value": object()}, TypeError),
        ({"bad\x00key": "value"}, ValueError),
    ],
)
def test_encoder_rejects_values_that_cannot_roundtrip(metadata, exception):
    with pytest.raises(exception):
        encode_metadata(metadata)


def test_encoder_keeps_values_and_nested_keys():
    assert json.loads(encode_metadata({"Site": "TAC", "nested": {"Keep": [True, 1, None]}})) == {
        "site": "TAC",
        "nested": {"Keep": [True, 1, None]},
    }
