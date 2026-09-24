"""Backend lifecycle and datasource parity over the in-memory iRODS transport."""

from contextlib import contextmanager
from copy import deepcopy

import numpy as np
import pandas as pd
import pytest
import xarray as xr

pytest.importorskip("irods")
from irods.exception import CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME

from test_irods_storage import remote as remote  # noqa: F401 - shared transport fixture

from openghg.objectstore._irods import IRODSObjectStore, irods_object_store
from openghg.objectstore._irods_storage import read_document
from openghg.types import DataOverlapError, ObjectStoreError


@pytest.fixture
def backend(remote, monkeypatch):
    """Add exclusive collection creation and lock removal to the shared transport."""
    create = remote.session.collections.create.side_effect
    remove = remote.session.collections.remove.side_effect

    def create_collection(path, recurse=True, **options):
        if not recurse and remote.session.collections.exists(path):
            raise CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME()
        return create(path, recurse=recurse, **options)

    def remove_collection(path, recurse=True, force=False, **options):
        if force:
            assert path.endswith("/.openghg-write-lock")
        remove(path, recurse=recurse, force=False, **options)
        for key in list(remote.avus):
            if key == path or key.startswith(path + "/"):
                del remote.avus[key]

    remote.session.collections.create.side_effect = create_collection
    remote.session.collections.remove.side_effect = remove_collection
    monkeypatch.setattr("openghg.objectstore._irods.validate_ordinary_collection", lambda session, path: path)
    return IRODSObjectStore(
        None, remote.root, remote.cache, mode="rw", data_type="surface", session_factory=remote.factory
    )


def dataset(start="2020-01-01", values=(1.0, 2.0, 3.0)):
    return xr.Dataset(
        {"mf": ("time", np.array(values))},
        coords={"time": pd.date_range(start, periods=len(values), freq="h")},
        attrs={"units": "ppb"},
    )


def test_factory_reads_without_cache_by_default(backend, remote, monkeypatch):
    original = dataset()
    monkeypatch.setattr("irods.session.iRODSSession", lambda **kwargs: remote.factory())
    store = irods_object_store(bucket=remote.root, data_type="surface", mode="rw")
    assert store.cache_dir is None
    with store:
        uuid = store.create({"species": "ch4"}, original, period="3600s")
    lazy = store.get_datasource(uuid).get_data()
    with store:
        assert store.search(species="ch4")[0]["uuid"] == uuid
    xr.testing.assert_equal(lazy.load(), original)
    assert not remote.cache.exists()


def test_factory_mirror_reads_after_context_exit_without_changing_publication(backend, remote, monkeypatch):
    connection_options = []

    def session(**kwargs):
        connection_options.append(kwargs)
        return remote.factory()

    monkeypatch.setattr("irods.session.iRODSSession", session)
    source = irods_object_store(bucket=remote.root, data_type="surface", mode="rw")
    with source:
        uuid = source.create({"species": "ch4"}, dataset(), period="3600s")
    before = deepcopy(remote.payloads), deepcopy(remote.avus), dict(remote.ids)
    remote.session.collections.create.reset_mock()
    mirror = irods_object_store(
        bucket=remote.root,
        data_type="surface",
        read_resource="mirrorResc",
        replicate_on_read=True,
    )
    with mirror:
        datasource = mirror.get_datasource(uuid)
        lazy = datasource.get_data()
    assert not mirror._locked
    xr.testing.assert_equal(lazy.load(), dataset())
    assert remote.session.data_objects.replicate.call_count > 0
    assert (remote.payloads, remote.avus, remote.ids) == before
    assert not remote.cache.exists()
    remote.session.collections.create.assert_not_called()
    assert all(
        "read_resource" not in options and "replicate_on_read" not in options
        for options in connection_options
    )
    assert remote.state.opened == remote.state.closed


def test_explicit_replication_reuses_good_targets_without_a_writer_lock(backend, remote):
    with backend:
        uuid = backend.create({"species": "ch4"}, dataset(), period="3600s")
    reader = IRODSObjectStore(None, remote.root, mode="r", session_factory=remote.factory)
    before = deepcopy(remote.avus), dict(remote.ids)
    remote.session.collections.create.reset_mock()
    with reader:
        reader.replicate(uuid, "mirrorResc")
        count = remote.session.data_objects.replicate.call_count
        assert count == len(remote.payloads)
    reader.replicate(uuid, "mirrorResc")
    assert remote.session.data_objects.replicate.call_count == count
    remote.session.collections.create.assert_not_called()
    assert (remote.avus, remote.ids) == before
    assert remote.session.numThreads == 4


def test_replication_pins_payloads_while_a_writer_publishes_new_data(backend, remote):
    from openghg.objectstore.irods_mirror import sync_replicas

    original = dataset()
    replacement = original.assign(mf=original.mf * 10)
    with backend:
        uuid = backend.create({"species": "ch4"}, original, period="3600s")
    reader = IRODSObjectStore(
        None, remote.root, mode="r", read_resource="mirrorResc", session_factory=remote.factory
    )
    pinned = reader.get_datasource(uuid)
    published_paths = set(remote.payloads)
    replicate = remote.session.data_objects.replicate.side_effect
    updated = False

    def replicate_with_concurrent_update(path, **options):
        nonlocal updated
        assert not reader._locked
        if not updated:
            updated = True
            with backend:
                backend.update(uuid, data=replacement, if_exists="new", new_version=False)
        return replicate(path, **options)

    remote.session.data_objects.replicate.side_effect = replicate_with_concurrent_update
    with reader:
        report = sync_replicas(reader, "mirrorResc", [uuid])
    assert updated
    assert set(remote.replicas) == published_paths
    assert not report["complete"]
    assert report["datasources"][0]["revision"] != pinned.revision
    assert report["datasources"][0]["missing_objects"] > 0
    xr.testing.assert_equal(pinned.get_data().load(), original)
    with reader:
        assert sync_replicas(reader, "mirrorResc", [uuid], verify=True)["complete"]
    xr.testing.assert_equal(reader.get_datasource(uuid).get_data().load(), replacement)


def test_mirror_writer_does_not_replicate_unpublished_generations(backend, remote):
    store = IRODSObjectStore(
        None,
        remote.root,
        mode="rw",
        resource="demoResc",
        read_resource="mirrorResc",
        replicate_on_read=True,
        session_factory=remote.factory,
    )
    with store:
        uuid = store.create({"species": "ch4"}, dataset(), period="3600s")
        remote.session.data_objects.replicate.assert_not_called()
        assert all(
            call.kwargs["destRescName"] == "demoResc"
            for call in remote.session.data_objects.put.call_args_list
        )
        published = set(remote.payloads)
        datasource = store.get_datasource(uuid)
        datasource.add(dataset("2020-01-02"), if_exists="combine", new_version=False)
        assert all(call.args[0] in published for call in remote.session.data_objects.replicate.call_args_list)
        datasource.save()
    xr.testing.assert_equal(
        datasource.get_data().load(),
        xr.concat([dataset(), dataset("2020-01-02")], dim="time"),
    )


@pytest.mark.parametrize(
    "options",
    [
        {"read_resource": ""},
        {"read_resource": "   "},
        {"read_resource": 1},
        {"replicate_on_read": True},
        {"replicate_on_read": "false"},
    ],
)
def test_backend_rejects_invalid_mirror_options_before_connecting(remote, options):
    opened = remote.state.opened
    with pytest.raises(ValueError):
        IRODSObjectStore(None, remote.root, session_factory=remote.factory, **options)
    assert remote.state.opened == opened


def test_context_reentry_catalog_state_and_deferred_lazy_reads(backend, remote):
    original = dataset()
    with backend:
        uuid = backend.create({"species": "ch4"}, original, period="3600s")
        datasource = backend.get_datasource(uuid)
        state = read_document(backend._sessions, backend.metastore.path(uuid), "publication")["datasource"]
        assert not (set(state) & datasource._runtime_state_keys)
        assert state["_uuid"] == uuid
        assert state["_latest_version"] == "v1"
        backend.write_document("surface/state", {"stored": True})
        assert backend.read_document("surface/state") == {"stored": True}

    assert remote.state.active == 0
    downloads = remote.state.downloads
    assert backend.search(species="ch4")[0]["uuid"] == uuid
    assert remote.state.downloads == downloads
    lazy = datasource.get_data()
    assert remote.state.active == 0
    xr.testing.assert_equal(lazy.load(), original)
    with backend:
        backend.update(uuid, metadata={"comment": "changed"})
    assert backend.search(comment="changed")[0]["uuid"] == uuid
    assert remote.state.opened == remote.state.closed


def test_version_overlap_attribute_edit_and_delete_parity(backend, remote):
    original = dataset()
    extension = dataset("2020-01-01T03:00", values=(4.0, 5.0))
    with backend:
        uuid = backend.create({"species": "ch4"}, original, period="3600s")
        with pytest.raises(DataOverlapError):
            backend.update(uuid, data=original)
        assert backend.get_datasource(uuid).latest_version == "v1"
        backend.update(uuid, data=extension, if_exists="combine", new_version=True)
        datasource = backend.get_datasource(uuid)
        assert datasource.latest_version == "v2"
        xr.testing.assert_equal(datasource.get_data("v1").load(), original)
        xr.testing.assert_equal(datasource.get_data().load(), xr.concat([original, extension], dim="time"))
        assert datasource.update_attributes(version="v1", data_vars="mf", to_update={"comment": "old"})
        datasource.save()
        assert datasource.get_data("v1").mf.attrs["comment"] == "old"
        assert "comment" not in datasource.get_data("v2").mf.attrs
        datasource.delete_version("v1")
        datasource.save()
        assert set(backend.get_datasource(uuid)._store.versions) == {"v2"}
        assert set(backend.search(uuid=uuid)[0]["versions"]) == {"v2"}
        backend.delete(uuid)
        assert backend.uuids == []
        assert remote.session.collections.exists(backend.metastore.path(uuid))
    assert remote.payloads  # Immutable generations outlive logical deletion.
    assert remote.state.opened == remote.state.closed


def test_native_readonly_access_does_not_change_remote_state(backend, remote):
    with backend:
        uuid = backend.create({"species": "ch4"}, dataset(), period="3600s")
    readonly = IRODSObjectStore(
        None, remote.root, remote.cache, data_type="surface", session_factory=remote.factory
    )
    before = deepcopy(remote.payloads), deepcopy(remote.avus)
    with readonly:
        datasource = readonly.get_datasource(uuid)
        xr.testing.assert_equal(datasource.get_data().load(), dataset())
        for mutation in (
            lambda: readonly.create({}, dataset()),
            lambda: readonly.update(uuid, metadata={"comment": "bad"}),
            lambda: readonly.delete(uuid),
            lambda: readonly.write_document("state", {}),
            lambda: datasource.add(dataset()),
            lambda: datasource.update_attributes(to_update={"comment": "bad"}),
            datasource.delete,
            datasource.save,
        ):
            with pytest.raises(PermissionError):
                mutation()
    assert (remote.payloads, remote.avus) == before
    assert remote.state.opened == remote.state.closed


def test_exclusive_lock_nested_context_and_exception_cleanup(backend, remote):
    other = IRODSObjectStore(None, remote.root, remote.cache, mode="rw", session_factory=remote.factory)
    with backend:
        with pytest.raises(ObjectStoreError, match="writer lock"):
            with other:
                pass
        assert other._sessions.active is None
        with backend:
            assert backend._locked
        assert backend._locked
        assert remote.session.collections.exists(backend.lock_path)
    assert not remote.session.collections.exists(backend.lock_path)
    with pytest.raises(RuntimeError, match="body failed"):
        with backend:
            raise RuntimeError("body failed")
    assert not backend._locked
    assert backend._sessions.active is None
    assert remote.state.opened == remote.state.closed
    backend.close()


@pytest.mark.parametrize("operation", ["add", "delete", "attributes", "mapping", "metastore"])
def test_escaped_writable_datasource_cannot_mutate_without_lock(backend, remote, operation):
    with backend:
        uuid = backend.create({"species": "ch4"}, dataset(), period="3600s")
        datasource = backend.get_datasource(uuid)
    before = deepcopy(remote.payloads), deepcopy(remote.avus)
    actions = {
        "add": lambda: datasource.add(dataset("2020-01-02"), if_exists="combine"),
        "delete": datasource.delete,
        "attributes": lambda: datasource.update_attributes(to_update={"comment": "bad"}),
        "mapping": lambda: datasource.mapping().__setitem__("unsafe", b"bad"),
        "metastore": lambda: backend.metastore.update({"uuid": uuid}, {"comment": "bad"}),
    }
    with pytest.raises(PermissionError if operation == "mapping" else ObjectStoreError):
        actions[operation]()
    assert (remote.payloads, remote.avus) == before
    assert remote.state.opened == remote.state.closed


def test_failed_lock_release_closes_connection_and_disallows_writes(backend, remote):
    remove = remote.session.collections.remove.side_effect

    def fail_release(path, **kwargs):
        if path == backend.lock_path:
            raise RuntimeError("cannot remove lock")
        return remove(path, **kwargs)

    remote.session.collections.remove.side_effect = fail_release
    with pytest.raises(RuntimeError, match="cannot remove lock"):
        with backend:
            pass
    assert not backend._locked
    assert backend._sessions.active is None
    with pytest.raises(ObjectStoreError, match="writer lock"):
        backend.write_document("state", {})
    backend.close()
    assert remote.state.opened == remote.state.closed


def test_borrowed_session_is_not_closed(backend, remote):
    with remote.factory() as borrowed:
        with IRODSObjectStore(borrowed, remote.root, remote.cache) as store:
            assert store.search() == []
        assert remote.state.active == 1
        borrowed.cleanup.assert_not_called()


def test_session_cleanup_failure_discards_context_before_deferred_reads(backend, remote):
    @contextmanager
    def failing_factory():
        with remote.factory() as session:
            yield session
        raise RuntimeError("session cleanup failed")

    backend._sessions.factory = failing_factory
    with pytest.raises(RuntimeError, match="session cleanup failed"):
        with backend:
            pass
    assert backend._sessions.active is None
    assert backend._connection is None
    assert not backend._locked
    backend.close()
    backend._sessions.factory = remote.factory
    assert backend.search() == []
    assert remote.state.opened == remote.state.closed


def test_failed_datasource_state_write_does_not_publish_a_searchable_record(backend, monkeypatch):
    from openghg.objectstore import _irods_metastore

    write = _irods_metastore.write_document

    def fail_state(session_factory, collection, key, value):
        if key == "publication":
            raise ObjectStoreError("cannot save datasource state")
        return write(session_factory, collection, key, value)

    monkeypatch.setattr(_irods_metastore, "write_document", fail_state)
    with backend:
        with pytest.raises(ObjectStoreError, match="cannot save datasource state"):
            backend.create({"species": "ch4"}, dataset(), period="3600s")
        assert backend.metastore.search() == []
