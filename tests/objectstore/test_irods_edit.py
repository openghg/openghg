"""Explicit edits retain iRODS publication and lazy-reader guarantees."""

from copy import deepcopy

import pytest
import xarray as xr

pytest.importorskip("irods")

from test_irods_backend import backend as backend, dataset  # noqa: F401
from test_irods_storage import remote as remote  # noqa: F401

from openghg.objectstore import PublicationConflictError
from openghg.objectstore import _irods_metastore
from openghg.types import ObjectStoreError


def test_batch_commit_publishes_once_and_preserves_saved_readers(backend, remote, monkeypatch):
    original = dataset()
    with backend:
        uuid = backend.create({"species": "ch4"}, original, period="3600s")
        source = backend.get_datasource(uuid)
        old = source.get_data()
        before = deepcopy(backend.metastore.publication(uuid))
        original_payloads = deepcopy(remote.payloads)
        publish = backend.metastore.publish
        publications = []

        def counted_publish(*args, **kwargs):
            publications.append(args)
            return publish(*args, **kwargs)

        monkeypatch.setattr(backend.metastore, "publish", counted_publish)
        with source.begin_edit() as edit:
            edit.append(dataset("2020-01-01T03:00", (4.0, 5.0)))
            edit.update(dataset("2020-01-01T01:00", (20.0,)))
            edit.update_attributes(to_update={"comment": "batch"})
            preview = edit.get_data()
            assert backend.metastore.publication(uuid) == before
            assert source.latest_version == "v1"
            assert edit.commit("one batch") == "v2"
        assert len(publications) == 1
        assert source.latest_version == "v2"
        assert source._commits["v2"]["parent"] == "v1"
        assert source._commits["v2"]["message"] == "one batch"
        assert backend.search(uuid=uuid)[0]["latest_version"] == "v2"
        assert all(remote.payloads[key] == value for key, value in original_payloads.items())
        current = source.get_data()
    expected = dataset(values=(1.0, 20.0, 3.0, 4.0, 5.0))
    expected.attrs["comment"] = "batch"
    xr.testing.assert_equal(old.load(), original)
    xr.testing.assert_equal(source.get_data("v1").load(), original)
    xr.testing.assert_equal(current.load(), expected)
    xr.testing.assert_equal(preview.load(), expected)
    assert remote.state.opened == remote.state.closed


def test_abort_and_owner_exit_discard_edits_without_publishing(backend, remote):
    with backend:
        uuid = backend.create({"species": "ch4"}, dataset(), period="3600s")
        source = backend.get_datasource(uuid)
        before = deepcopy(remote.payloads)
        publication = backend.metastore.publication(uuid)
        with source.begin_edit() as edit:
            edit.append(dataset("2020-01-02"))
        edit.abort()
        assert remote.payloads == before
        assert backend.metastore.publication(uuid) == publication
        escaped = source.begin_edit()
        escaped.append(dataset("2020-01-03"))
        escaped_store = escaped._payload.store.store
    assert remote.payloads == before
    with backend:
        with pytest.raises(RuntimeError, match="closed"):
            escaped.commit()
        with pytest.raises(ObjectStoreError, match="context has closed"):
            escaped_store["unsafe"] = b"bad"
        with source.begin_edit() as next_edit:
            next_edit.append(dataset("2020-01-04"))
            assert next_edit.commit() == "v2"


@pytest.mark.parametrize("operation", ["save", "add", "add_data", "add_timed_data", "attributes"])
def test_immutable_policy_rejects_legacy_writes_through_stale_handle(backend, operation):
    with backend:
        uuid = backend.create({"species": "ch4"}, dataset(), period="3600s")
        stale = backend.get_datasource(uuid)
        source = backend.get_datasource(uuid)
        with source.begin_edit() as edit:
            edit.append(dataset("2020-01-02"))
            edit.commit()
        actions = {
            "save": stale.save,
            "add": lambda: stale.add(dataset("2020-01-03")),
            "add_data": lambda: stale.add_data({}, dataset("2020-01-03"), "surface"),
            "add_timed_data": lambda: stale.add_timed_data(dataset("2020-01-03"), "surface", False, False),
            "attributes": lambda: stale.update_attributes(to_update={"comment": "old API"}),
        }
        with pytest.raises(ObjectStoreError, match="immutable.*begin_edit"):
            actions[operation]()


@pytest.mark.parametrize("lost_ack", [False, True])
def test_failed_publication_retains_generation_and_requires_reload(backend, remote, monkeypatch, lost_ack):
    with backend:
        uuid = backend.create({"species": "ch4"}, dataset(), period="3600s")
        source = backend.get_datasource(uuid)
        old = source.get_data()
        old_paths = dict(source._version_paths)
        edit = source.begin_edit()
        edit.append(dataset("2020-01-02"))
        staged_path = edit._payload.reference
        write = _irods_metastore.write_document

        def fail(*args, **kwargs):
            if lost_ack:
                write(*args, **kwargs)
            raise RuntimeError("publication failed")

        monkeypatch.setattr(_irods_metastore, "write_document", fail)
        with pytest.raises(RuntimeError, match="publication failed"):
            edit.commit()
        assert source.latest_version == "v1"
        assert source._version_paths == old_paths
        assert any(key.startswith(staged_path + "/") for key in remote.payloads)
        with pytest.raises(ObjectStoreError, match="reload"):
            source.begin_edit()
        reloaded = backend.get_datasource(uuid)
        assert reloaded.latest_version == ("v2" if lost_ack else "v1")
    xr.testing.assert_equal(old.load(), dataset())
    assert remote.state.opened == remote.state.closed


def test_stale_editor_cannot_publish_after_catalog_change(backend):
    with backend:
        uuid = backend.create({"species": "ch4"}, dataset(), period="3600s")
        source = backend.get_datasource(uuid)
        edit = source.begin_edit()
        edit.append(dataset("2020-01-02"))
        backend.metastore.update({"uuid": uuid}, {"comment": "changed"})
        with pytest.raises(PublicationConflictError):
            edit.commit()
        edit.abort()
        assert backend.get_datasource(uuid).latest_version == "v1"


@pytest.mark.parametrize("operation", ["version", "all_data", "datasource", "store", "update"])
def test_immutable_deletion_and_legacy_update_leave_publication_unchanged(backend, remote, operation):
    with backend:
        uuid = backend.create({"species": "ch4"}, dataset(), period="3600s")
        source = backend.get_datasource(uuid)
        with source.begin_edit() as edit:
            edit.append(dataset("2020-01-02"))
            edit.commit()
        before = deepcopy(remote.payloads), deepcopy(remote.avus)
        actions = {
            "version": lambda: source.delete_version("v1"),
            "all_data": source.delete_all_data,
            "datasource": source.delete,
            "store": lambda: backend.delete(uuid),
            "update": lambda: backend.update(
                uuid, metadata={"comment": "must not publish"}, data=dataset("2020-01-03")
            ),
        }
        with pytest.raises(ObjectStoreError, match="immutable.*begin_edit"):
            actions[operation]()
        assert (remote.payloads, remote.avus) == before
        assert backend.get_datasource(uuid).latest_version == "v2"
