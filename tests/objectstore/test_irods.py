"""Transport failure checks and an opt-in real iRODS round trip."""

import base64
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest
import xarray as xr

pytest.importorskip("irods")

from openghg.objectstore import IRODSObjectStore
from openghg.objectstore import _irods
from openghg.types import ObjectStoreError


def checksum(payload):
    return "sha2:" + base64.b64encode(hashlib.sha256(payload).digest()).decode()


@pytest.fixture
def transport(tmp_path, monkeypatch):
    """Use real serialization and cache files, with an in-memory remote transport."""
    records, payloads = {}, {}
    session = Mock(host="example.invalid", port=1247, zone="testZone")
    state = SimpleNamespace(downloads=0, corrupt=False, changed=False, object_id=17)

    def get(path, local_path=None, **options):
        payload = payloads[path]
        if local_path is not None:
            state.downloads += 1
            Path(local_path).write_bytes(b"bad" if state.corrupt else payload)
            if state.changed:
                state.object_id += 1
        replica = SimpleNamespace(
            number=0, status="1", resource_name="demoResc", checksum=checksum(payload), size=len(payload)
        )
        return SimpleNamespace(id=state.object_id, replicas=[replica])

    def put(local_path, path, **options):
        assert options["forceFlag"] is False
        assert path not in payloads
        payloads[path] = Path(local_path).read_bytes()

    def unlink(path, **options):
        del payloads[path]
        records.pop(Path(path).stem, None)

    session.data_objects.get.side_effect = get
    session.data_objects.put.side_effect = put
    session.data_objects.exists.side_effect = lambda path: path in payloads
    session.data_objects.unlink.side_effect = unlink
    catalog = Mock()
    catalog.path.side_effect = lambda uuid: f"/testZone/home/test/store/{uuid}.nc"
    catalog.record.side_effect = lambda uuid: dict(records[uuid])
    catalog.search.side_effect = lambda search_terms=None, **kwargs: [
        dict(r) for r in records.values() if all(r.get(k) == v for k, v in (search_terms or {}).items())
    ]
    catalog.insert.side_effect = lambda record: records.update({record["uuid"]: dict(record)})
    monkeypatch.setattr(_irods, "IRODSMetaStore", lambda *args, **kwargs: catalog)
    store = IRODSObjectStore(session, "/testZone/home/test/store", tmp_path / "cache", mode="rw")
    return SimpleNamespace(
        store=store,
        session=session,
        catalog=catalog,
        records=records,
        payloads=payloads,
        state=state,
        cache=tmp_path / "cache",
    )


def test_publish_search_and_verified_read_through_cache(transport):
    t = transport
    data = xr.Dataset({"mf": ("time", [1.0, 2.0])}, coords={"time": [0, 1]})
    metadata = {"species": "ch4", "revision": "v1"}
    uuid = t.store.create(metadata, data)
    assert metadata == {"species": "ch4", "revision": "v1"}
    assert t.store.uuids == [uuid]
    datasource = t.store.retrieve(species="ch4")[0]
    assert t.state.downloads == 0
    assert not t.cache.exists()
    xr.testing.assert_identical(datasource.get_data(), data)
    path = datasource.local_path()
    receipt = datasource.provenance
    assert receipt["kind"] == "verified_local_cache"
    assert receipt["source"]["data_id"] == "17"
    assert receipt["source"]["checksum"] == checksum(path.read_bytes())
    assert receipt["uuid"] == uuid
    assert json.loads(path.with_name("provenance.json").read_text()) == receipt
    assert t.state.downloads == 1

    # A new process/store instance reuses the persisted, verified local snapshot.
    second = IRODSObjectStore(t.session, "/testZone/home/test/store", t.cache)
    xr.testing.assert_identical(second.get_datasource(uuid).get_data(), data)
    assert t.state.downloads == 1
    t.store.close()
    t.session.cleanup.assert_not_called()


def test_cache_corruption_and_remote_identity_change(transport):
    t = transport
    uuid = t.store.create({"revision": 1}, xr.Dataset({"x": 1}))
    datasource = t.store.get_datasource(uuid)
    path = datasource.local_path()
    path.write_bytes(b"corrupt")
    datasource.get_data()
    assert t.state.downloads == 2
    original = path.read_bytes()
    t.state.object_id += 1  # Same path, different catalog identity.
    assert datasource.local_path() != path
    assert path.read_bytes() == original
    assert t.state.downloads == 3


@pytest.mark.parametrize("contents", ["null", "[]", "{}", "invalid json"])
def test_corrupt_receipt_is_rebuilt(transport, contents):
    t = transport
    uuid = t.store.create({"revision": 1}, xr.Dataset({"x": 1}))
    datasource = t.store.get_datasource(uuid)
    path = datasource.local_path()
    path.with_name("provenance.json").write_text(contents)
    assert datasource.provenance["uuid"] == uuid
    assert t.state.downloads == 2


@pytest.mark.parametrize("failure", ["corrupt", "changed"])
def test_failed_download_never_publishes_cache(transport, failure):
    t = transport
    uuid = t.store.create({"revision": 1}, xr.Dataset({"x": 1}))
    setattr(t.state, failure, True)
    with pytest.raises(ObjectStoreError):
        t.store.get_datasource(uuid).local_path()
    assert not list(t.cache.rglob("data.nc"))
    assert not list(t.cache.rglob("provenance.json"))


def test_failed_metadata_publication_removes_only_new_object(transport):
    t = transport
    old_uuid = t.store.create({"revision": 1}, xr.Dataset({"x": 1}))
    t.catalog.insert.side_effect = RuntimeError("catalog unavailable")
    with pytest.raises(RuntimeError, match="catalog unavailable"):
        t.store.create({"revision": 2}, xr.Dataset({"x": 2}))
    assert list(t.records) == [old_uuid]
    assert list(t.payloads) == [t.catalog.path(old_uuid)]
    assert t.session.data_objects.unlink.call_args.kwargs == {"force": True}


def test_uncertain_upload_failure_does_not_delete_another_object(transport):
    from irods.exception import OVERWRITE_WITHOUT_FORCE_FLAG

    t = transport

    def collision(local_path, path, **options):
        t.payloads[path] = b"another writer's object"
        raise OVERWRITE_WITHOUT_FORCE_FLAG

    t.session.data_objects.put.side_effect = collision
    with pytest.raises(OVERWRITE_WITHOUT_FORCE_FLAG):
        t.store.create({"revision": 1}, xr.Dataset({"x": 1}))
    t.session.data_objects.unlink.assert_not_called()
    assert list(t.payloads.values()) == [b"another writer's object"]


def test_invalid_metadata_and_serialization_do_not_upload(transport):
    t = transport
    with pytest.raises((ValueError, TypeError)):
        t.store.create({"bad": object()}, xr.Dataset())
    with pytest.raises(TypeError):
        t.store.create({"revision": 1}, xr.Dataset(attrs={"bad": object()}))
    t.session.data_objects.put.assert_not_called()


def test_payload_update_rejected_before_metadata_changes(transport):
    t = transport
    uuid = t.store.create({"revision": 1}, xr.Dataset({"x": 1}))
    with pytest.raises(NotImplementedError):
        t.store.update(uuid, metadata={"revision": 2}, data=xr.Dataset())
    assert t.records[uuid]["revision"] == 1
    t.catalog.update.assert_not_called()


def test_readonly_and_managed_replication_are_explicit(transport):
    t = transport
    uuid = t.store.create({"revision": 1}, xr.Dataset({"x": 1}))
    t.store.replicate(uuid, "secondResource")
    t.session.data_objects.replicate.assert_called_once_with(t.catalog.path(uuid), resource="secondResource")
    reader = IRODSObjectStore(t.session, "/testZone/home/test/store", t.cache)
    for operation in (
        lambda: reader.create({}, xr.Dataset()),
        lambda: reader.update(uuid, metadata={"revision": 2}),
        lambda: reader.delete(uuid),
        lambda: reader.replicate(uuid, "secondResource"),
    ):
        with pytest.raises(PermissionError):
            operation()
    path = t.store.get_datasource(uuid).local_path()
    t.store.delete(uuid)
    assert path.exists()
    t.session.data_objects.unlink.assert_called_with(t.catalog.path(uuid), force=False)


def test_live_irods_round_trip(tmp_path):
    """Run only against a caller-supplied disposable collection, using real APIs."""
    root = os.environ.get("OPENGHG_IRODS_TEST_COLLECTION")
    if not root:
        pytest.skip("Set OPENGHG_IRODS_TEST_COLLECTION for the live iRODS test.")
    from irods.session import iRODSSession

    env_file = os.environ.get("IRODS_ENVIRONMENT_FILE", os.path.expanduser("~/.irods/irods_environment.json"))
    collection = root.rstrip("/") + "/openghg-test-" + str(uuid4())
    with iRODSSession(irods_env_file=env_file) as session:
        session.collections.create(collection)
        try:
            writer = IRODSObjectStore(session, collection, tmp_path, mode="rw")
            data = xr.Dataset({"mf": ("time", [1.0, 2.0])}, coords={"time": [0, 1]})
            uuid = writer.create({"species": "ch4", "revision": 1}, data)
            assert writer.search(species="ch4")[0]["uuid"] == uuid
            assert not list(tmp_path.rglob("data.nc"))
            reader = IRODSObjectStore(session, collection, tmp_path)
            datasource = reader.retrieve(species="ch4")[0]
            xr.testing.assert_identical(datasource.get_data(), data)
            assert datasource.provenance["uuid"] == uuid
            writer.update(uuid, metadata={"site": "tac"})
            assert reader.search(site="tac")[0]["uuid"] == uuid
            target_resource = os.environ.get("OPENGHG_IRODS_TEST_RESOURCE")
            if target_resource:
                writer.replicate(uuid, target_resource)
                assert any(
                    r.resource_name == target_resource
                    for r in session.data_objects.get(writer.metastore.path(uuid)).replicas
                )
            writer.delete(uuid)
            assert reader.search() == []
        finally:
            session.collections.remove(collection, recurse=True, force=True)
