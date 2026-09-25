"""Publication, conflict, and lazy-reader guarantees over the fake iRODS transport."""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

import pytest
import xarray as xr
import zarr

pytest.importorskip("irods")

from test_irods_backend import backend as backend, dataset  # noqa: F401
from test_irods_storage import remote as remote  # noqa: F401

from openghg.objectstore import PublicationConflictError
from openghg.objectstore._irods import IRODSObjectStore
from openghg.objectstore import _irods_metastore
from openghg.objectstore._irods_storage import IRODSKVStore, IRODSZarrMapping, delete_document, write_document
from openghg.types import ObjectStoreError


def second_handle(backend, remote):
    return IRODSObjectStore(None, remote.root, remote.cache, mode="rw", session_factory=remote.factory)


def test_readers_keep_complete_snapshot_during_and_after_same_version_update(backend, remote, monkeypatch):
    original = dataset()
    replacement = dataset(values=(10.0, 20.0, 30.0))
    with backend:
        uuid = backend.create({"species": "ch4", "comment": "before"}, original, period="3600s")
    pinned = backend.get_datasource(uuid)
    lazy = pinned.get_data()
    old_path = pinned.mapping().collection
    before = deepcopy(remote.payloads)
    write = _irods_metastore.write_document
    observed = []

    def inspect_before_publication(factory, collection, key, value):
        if key == "publication":
            # Payload preparation is complete, but search/state/data all remain old.
            current = backend.get_datasource(uuid)
            assert current.revision == pinned.revision
            assert backend.search(uuid=uuid)[0]["comment"] == "before"
            xr.testing.assert_equal(current.get_data().load(), original)
            observed.append(True)
        write(factory, collection, key, value)

    monkeypatch.setattr(_irods_metastore, "write_document", inspect_before_publication)
    with backend:
        backend.update(
            uuid, metadata={"comment": "after"}, data=replacement, if_exists="new", new_version=False
        )
    assert observed
    current = backend.get_datasource(uuid)
    assert current.latest_version == "v1"
    assert current.mapping().collection != old_path
    assert current.revision != pinned.revision
    assert all(remote.payloads[path] == content for path, content in before.items())
    xr.testing.assert_equal(lazy.load(), original)
    xr.testing.assert_equal(pinned.get_data().load(), original)
    xr.testing.assert_equal(current.get_data().load(), replacement)
    assert backend.search(uuid=uuid)[0]["comment"] == "after"


def test_payload_failure_cannot_publish_metadata_or_corrupt_old_readers(backend, remote):
    with backend:
        uuid = backend.create({"species": "ch4"}, dataset(), period="3600s")
    pinned = backend.get_datasource(uuid)
    before = backend.metastore.publication(uuid)
    put = remote.session.data_objects.put.side_effect
    count = 0

    def partial_failure(*args, **kwargs):
        nonlocal count
        count += 1
        if count == 3:
            raise RuntimeError("interrupted upload")
        return put(*args, **kwargs)

    remote.session.data_objects.put.side_effect = partial_failure
    with backend:
        with pytest.raises(RuntimeError, match="interrupted upload"):
            backend.update(
                uuid,
                metadata={"comment": "must remain invisible"},
                data=dataset("2020-01-02"),
                if_exists="combine",
                new_version=False,
            )
    assert backend.metastore.publication(uuid) == before
    assert not backend.search(comment="must remain invisible")
    xr.testing.assert_equal(pinned.get_data().load(), dataset())


def test_failed_manifest_write_leaves_old_snapshot_and_allows_reload(backend, monkeypatch):
    with backend:
        uuid = backend.create({"species": "ch4"}, dataset(), period="3600s")
    pinned = backend.get_datasource(uuid)
    write = _irods_metastore.write_document

    def fail(*args, **kwargs):
        raise RuntimeError("atomic publication failed")

    monkeypatch.setattr(_irods_metastore, "write_document", fail)
    with backend:
        with pytest.raises(RuntimeError, match="atomic publication failed"):
            backend.update(uuid, data=dataset("2020-01-02"), if_exists="new", new_version=False)
    assert backend.get_datasource(uuid).revision == pinned.revision
    xr.testing.assert_equal(pinned.get_data().load(), dataset())
    monkeypatch.setattr(_irods_metastore, "write_document", write)
    with backend:
        backend.update(uuid, data=dataset("2020-01-02"), if_exists="new", new_version=False)
    xr.testing.assert_equal(backend.get_datasource(uuid).get_data().load(), dataset("2020-01-02"))


@pytest.mark.parametrize("operation", ["save", "add", "attributes", "delete_version", "delete", "update"])
def test_stale_writers_fail_before_payload_changes(backend, remote, operation):
    with backend:
        uuid = backend.create({"species": "ch4"}, dataset(), period="3600s")
    stale = backend.get_datasource(uuid)
    with backend:
        backend.metastore.update({"uuid": uuid}, {"comment": "concurrent"})
    before = deepcopy(remote.payloads), deepcopy(remote.avus)
    actions = {
        "save": stale.save,
        "add": lambda: stale.add(dataset("2020-01-02")),
        "attributes": lambda: stale.update_attributes(to_update={"comment": "stale"}),
        "delete_version": lambda: stale.delete_version("v1"),
        "delete": stale.delete,
        "update": lambda: backend.update(
            uuid, metadata={"comment": "stale"}, expected_revision=stale.revision
        ),
    }
    with backend:
        with pytest.raises(PublicationConflictError, match="reload"):
            actions[operation]()
    assert (remote.payloads, remote.avus) == before
    assert backend.search(uuid=uuid)[0]["comment"] == "concurrent"


def test_retained_mapping_cannot_modify_a_committed_generation(backend):
    with backend:
        uuid = backend.create({"species": "ch4"}, dataset(), period="3600s")
        source = backend.get_datasource(uuid)
        source.add(dataset("2020-01-02"), if_exists="new", new_version=False)
        staged = source._store.store
        source.save()
        with pytest.raises(PermissionError, match="immutable"):
            staged["unsafe"] = b"bad"
        with pytest.raises(PermissionError, match="read-only"):
            source.mapping()["unsafe"] = b"bad"


def test_deleted_versions_and_datasources_remain_readable_by_pinned_readers(backend, remote):
    with backend:
        uuid = backend.create({"species": "ch4"}, dataset(), period="3600s")
        backend.update(uuid, data=dataset("2020-01-02"), if_exists="new")
    snapshot = backend.get_datasource(uuid)
    old_version, latest = snapshot.get_data("v1"), snapshot.get_data()
    with backend:
        current = backend.get_datasource(uuid)
        current.delete_version("v1")
        current.save()
        backend.delete(uuid)
    assert backend.search() == []
    with pytest.raises(ObjectStoreError, match="published"):
        backend.get_datasource(uuid)
    xr.testing.assert_equal(old_version.load(), dataset())
    xr.testing.assert_equal(latest.load(), dataset("2020-01-02"))
    assert remote.payloads
    assert backend.metastore.publication(uuid)["record"] is None
    with backend:
        with pytest.raises(PublicationConflictError):
            snapshot.save()


def test_store_document_stale_save_is_rejected_and_tokens_do_not_leak(backend, remote):
    other = second_handle(backend, remote)
    assert backend.read_document("state") is None
    assert other.read_document("state") is None
    with backend:
        backend.write_document("state", {"history": ["first"]})
    with other:
        with pytest.raises(PublicationConflictError, match="document"):
            other.write_document("state", {"history": ["lost update"]})
    assert other.read_document("state") == {"history": ["first"]}
    with other:
        other.write_document("state", {"history": ["first", "second"]})
    with backend:
        with pytest.raises(PublicationConflictError):
            backend.write_document("state", {"history": ["stale"]})
    assert backend.read_document("state") == {"history": ["first", "second"]}


def test_legacy_layout_migrates_without_overwriting_legacy_generation(backend, remote):
    with backend:
        uuid = backend.create({"species": "ch4"}, dataset(), period="3600s")
        manifest = backend.metastore.publication(uuid)
        path = backend.metastore.path(uuid)
        legacy = IRODSZarrMapping(remote.factory, path + "/v1", remote.cache)
        zarr.copy_store(backend.get_datasource(uuid)._store.store, IRODSKVStore(legacy))
        write_document(remote.factory, path, "record", manifest["record"])
        write_document(remote.factory, path, "datasource", manifest["datasource"])
        delete_document(remote.factory, path, "publication")
    old = backend.get_datasource(uuid)
    assert old.revision.startswith("legacy:")
    assert old.mapping().collection == path + "/v1"
    with backend:
        backend.update(uuid, data=dataset(values=(8.0, 9.0, 10.0)), if_exists="new", new_version=False)
    assert not backend.get_datasource(uuid).revision.startswith("legacy:")
    assert backend.get_datasource(uuid).mapping().collection != path + "/v1"
    xr.testing.assert_equal(old.get_data().load(), dataset())


def test_shared_handle_rejects_foreign_thread_context_and_mutations(backend):
    with backend:
        with ThreadPoolExecutor(max_workers=1) as pool:
            for action in (backend.__enter__, lambda: backend.write_document("state", {}), backend.close):
                with pytest.raises(ObjectStoreError, match="thread"):
                    pool.submit(action).result()
        assert backend._locked
        backend.write_document("state", {"owner": "unchanged"})
    assert backend.read_document("state") == {"owner": "unchanged"}


def test_lost_publication_acknowledgement_still_freezes_uploaded_generation(backend, monkeypatch):
    with backend:
        uuid = backend.create({"species": "ch4"}, dataset(), period="3600s")
        source = backend.get_datasource(uuid)
        source.add(dataset("2020-01-02"), if_exists="new", new_version=False)
        staged = source._store.store
        write = _irods_metastore.write_document

        def lost_ack(*args, **kwargs):
            write(*args, **kwargs)
            raise RuntimeError("lost server acknowledgement")

        monkeypatch.setattr(_irods_metastore, "write_document", lost_ack)
        with pytest.raises(RuntimeError, match="acknowledgement"):
            source.save()
        with pytest.raises(PermissionError, match="immutable"):
            staged["unsafe"] = b"bad"
        with pytest.raises(ObjectStoreError, match="reload"):
            source.save()
    xr.testing.assert_equal(backend.get_datasource(uuid).get_data().load(), dataset("2020-01-02"))


def test_attribute_edit_does_not_change_pinned_generation_or_runtime_state(backend):
    with backend:
        uuid = backend.create({"species": "ch4"}, dataset(), period="3600s")
    old = backend.get_datasource(uuid)
    old_lazy = old.get_data()
    with backend:
        current = backend.get_datasource(uuid)
        current.update_attributes(to_update={"comment": "new attributes"})
        current.save()
    assert "comment" not in old_lazy.load().attrs
    assert "comment" not in old.get_data().attrs
    new = backend.get_datasource(uuid)
    assert new.get_data().attrs["comment"] == "new attributes"
    state = backend.metastore.publication(uuid)["datasource"]
    assert not set(state).intersection(new._runtime_state_keys)


def test_stale_raw_search_record_is_replaced_with_loaded_publication(backend, monkeypatch):
    with backend:
        uuid = backend.create({"species": "ch4", "comment": "old"}, dataset(), period="3600s")
    stale_record = backend.metastore.record(uuid)
    with backend:
        backend.update(uuid, metadata={"comment": "new"})
    search = backend.metastore.search

    def stale_catalog(*args, **kwargs):
        # Simulate a writer publishing between catalog candidate search and load.
        return [stale_record] if search(*args, **kwargs) else []

    monkeypatch.setattr(backend.metastore, "search", stale_catalog)
    assert backend.search(uuid=uuid)[0]["comment"] == "new"
    assert not backend.search(comment="old")


def test_session_factory_failure_releases_local_context_guard(backend, remote):
    def fail():
        raise RuntimeError("credentials unavailable")

    backend._sessions.factory = fail
    with pytest.raises(RuntimeError, match="credentials unavailable"):
        with backend:
            pass
    backend._sessions.factory = remote.factory
    with ThreadPoolExecutor(max_workers=1) as pool:

        def enter_from_other_thread():
            with backend:
                backend.write_document("state", {"recovered": True})

        pool.submit(enter_from_other_thread).result()
    assert backend.read_document("state") == {"recovered": True}


def test_delete_latest_updates_dates_and_append_after_version_gap_is_safe(backend):
    with backend:
        uuid = backend.create({"species": "ch4"}, dataset(), period="3600s")
        first = backend.get_datasource(uuid)
        backend.update(uuid, data=dataset("2020-01-03"), if_exists="new")
        latest = backend.get_datasource(uuid)
        latest.daterange()  # Fill the date cache before deleting its current version.
        latest.delete_version("v2")
        latest.save()
        result = backend.search(uuid=uuid)[0]
        assert result["latest_version"] == "v1"
        assert result["start_date"] == first.metadata["start_date"]
        assert result["end_date"] == first.metadata["end_date"]
        assert result["timestamp"] == first.metadata["timestamp"]
        assert latest.daterange() == first.daterange()
        backend.update(uuid, data=dataset("2020-01-04"), if_exists="new")
        latest = backend.get_datasource(uuid)
        latest.delete_version("v1")
        latest.save()
        backend.update(uuid, data=dataset("2020-01-05"), if_exists="new")
    latest = backend.get_datasource(uuid)
    assert latest.latest_version == "v3"
    xr.testing.assert_equal(latest.get_data("v2").load(), dataset("2020-01-04"))
    xr.testing.assert_equal(latest.get_data("v3").load(), dataset("2020-01-05"))


def test_failed_direct_add_data_cannot_publish_partial_metadata(backend):
    with backend:
        uuid = backend.create({"species": "ch4"}, dataset(), period="3600s")
        source = backend.get_datasource(uuid)
        with pytest.raises(NotImplementedError):
            source.add_data({"comment": "partial edit"}, xr.Dataset({"x": ("index", [1])}), "surface")
        with pytest.raises(ObjectStoreError, match="reload"):
            source.save()
    assert not backend.search(comment="partial edit")
