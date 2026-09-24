"""Exercise catalog documents and Zarr transport without an iRODS deployment."""

import base64
from contextlib import contextmanager
import hashlib
from io import BytesIO
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pytest
import xarray as xr
import zarr

pytest.importorskip("irods")
from irods.exception import CollectionDoesNotExist, DataObjectDoesNotExist
from irods.meta import iRODSMeta

from openghg.objectstore._irods_storage import (
    IRODSKVStore,
    IRODSZarrMapping,
    delete_document,
    document_attribute,
    list_collections,
    read_document,
    validate_ordinary_collection,
    write_document,
)
from openghg.types import ObjectStoreError
from openghg.storage._zarr_store import VersionedZarrStore


def checksum(payload):
    return "sha2:" + base64.b64encode(hashlib.sha256(payload).digest()).decode()


@pytest.fixture
def remote(tmp_path):
    """Use real cache files and client AVUs with an in-memory transport."""
    collections, payloads, ids, avus, replicas = set(), {}, {}, {}, {}
    state = SimpleNamespace(active=0, opened=0, closed=0, downloads=0, corrupt=False, changed=False)
    session = Mock(host="example.invalid", port=1247, numThreads=4)

    @contextmanager
    def factory():
        state.active += 1
        state.opened += 1
        try:
            yield session
        finally:
            state.closed += 1
            state.active -= 1

    @contextmanager
    def clone():
        cloned = Mock(numThreads=session.numThreads)

        def replicate(*args, **kwargs):
            assert cloned.numThreads == -1
            return session.data_objects.replicate(*args, **kwargs)

        cloned.data_objects.replicate.side_effect = replicate
        with factory():
            yield cloned

    session.clone.side_effect = clone

    def get_object(path, local_path=None, **options):
        assert state.active
        if path not in payloads:
            raise DataObjectDoesNotExist()
        payload = payloads[path]
        if local_path is not None:
            assert any(str(r.number) == options["replNum"] for r in get_object(path).replicas)
            state.downloads += 1
            Path(local_path).write_bytes(b"bad" if state.corrupt else payload)
            if state.changed:
                ids[path] += 1
        replica = SimpleNamespace(
            number=0,
            status="1",
            resource_name="demoResc",
            resc_hier="demoResc",
            checksum=checksum(payload),
            size=len(payload),
        )
        return SimpleNamespace(
            id=ids[path], path=path, size=len(payload), replicas=[replica, *replicas.get(path, [])]
        )

    def put_object(local_path, path, **options):
        assert state.active
        assert options["forceFlag"] is True
        payloads[path] = Path(local_path).read_bytes()
        ids.setdefault(path, len(ids) + 1)
        for replica in replicas.get(path, []):
            replica.status = "0"

    def replicate_object(path, **options):
        assert state.active
        obj = get_object(path)
        destination = options["destRescName"]
        matches = [r for r in replicas.get(path, []) if destination in r.resc_hier.split(";")]
        if matches:
            replica = matches[0]
        else:
            replica = SimpleNamespace(
                number=max(r.number for r in obj.replicas) + 1,
                resource_name=destination,
                resc_hier=destination,
            )
            replicas.setdefault(path, []).append(replica)
        replica.status = "1"
        replica.checksum = checksum(payloads[path])
        replica.size = len(payloads[path])

    def open_object(path, mode, **options):
        assert state.active
        assert mode == "r"
        if path not in payloads:
            raise DataObjectDoesNotExist()
        assert any(str(r.number) == options["replNum"] for r in get_object(path).replicas)
        state.downloads += 1
        payload = b"bad" if state.corrupt else payloads[path]
        if state.changed:
            ids[path] += 1
        return BytesIO(payload)

    def create_collection(path, **options):
        assert state.active
        collections.update(
            str(p) for p in [PurePosixPath(path), *PurePosixPath(path).parents] if str(p) != "/"
        )
        return get_collection(path)

    def get_collection(path):
        assert state.active
        if path not in collections:
            raise CollectionDoesNotExist()
        metadata = Mock()
        metadata.get_all.side_effect = lambda name: [a for a in avus.get(path, []) if a.name == name]

        def apply(*operations):
            replacement = list(avus.get(path, []))
            for operation in operations:
                if operation.operation == "remove":
                    replacement.remove(operation.avu)
                else:
                    replacement.append(operation.avu)
            avus[path] = replacement

        metadata.apply_atomic_operations.side_effect = apply

        class Collection:
            @property
            def subcollections(self):
                return [
                    get_collection(c) for c in sorted(collections) if str(PurePosixPath(c).parent) == path
                ]

            @property
            def data_objects(self):
                return [get_object(p) for p in payloads if str(PurePosixPath(p).parent) == path]

            def walk(self):
                yield self, self.subcollections, self.data_objects
                for subcollection in self.subcollections:
                    yield from subcollection.walk()

        collection = Collection()
        collection.path, collection.metadata = path, metadata
        return collection

    def remove_collection(path, **options):
        assert options["force"] is False
        for key in list(payloads):
            if key.startswith(path + "/"):
                del payloads[key]
        collections.difference_update(c for c in list(collections) if c == path or c.startswith(path + "/"))

    session.data_objects.get.side_effect = get_object
    session.data_objects.put.side_effect = put_object
    session.data_objects.open.side_effect = open_object
    session.data_objects.replicate.side_effect = replicate_object
    session.data_objects.exists.side_effect = lambda path: path in payloads
    session.data_objects.unlink.side_effect = lambda path, **options: payloads.pop(path)
    session.collections.get.side_effect = get_collection
    session.collections.create.side_effect = create_collection
    session.collections.exists.side_effect = lambda path: path in collections
    session.collections.remove.side_effect = remove_collection
    root = "/testZone/home/test/store"
    with factory():
        create_collection(root)
    return SimpleNamespace(
        factory=factory,
        session=session,
        root=root,
        state=state,
        avus=avus,
        payloads=payloads,
        ids=ids,
        replicas=replicas,
        cache=tmp_path / "cache",
    )


def mapping(remote, **kwargs):
    return IRODSZarrMapping(remote.factory, remote.root + "/v1", remote.cache, **kwargs)


def test_large_documents_atomic_replacement_and_namespace(remote):
    t = remote
    t.avus[t.root] = [iRODSMeta("external", "preserve me")]
    first = {"history": ["αβ🙂" * 700, "a'b\\c"], "revision": 1}
    write_document(t.factory, t.root, "datasource", first)
    write_document(t.factory, t.root, "record", {"species": "ch4"})
    assert read_document(t.factory, t.root, "datasource") == first
    chunks = [a for a in t.avus[t.root] if a.name == document_attribute("datasource")]
    assert len(chunks) > 2
    assert all(len(a.value.encode()) <= 2500 for a in chunks)
    write_document(t.factory, t.root, "datasource", {"revision": 2})
    assert read_document(t.factory, t.root, "datasource") == {"revision": 2}
    assert read_document(t.factory, t.root, "record") == {"species": "ch4"}
    delete_document(t.factory, t.root, "datasource")
    delete_document(t.factory, t.root, "datasource")
    with pytest.raises(KeyError):
        read_document(t.factory, t.root, "datasource")
    assert iRODSMeta("external", "preserve me") in t.avus[t.root]
    assert t.state.opened == t.state.closed


def test_missing_and_corrupt_documents(remote):
    t = remote
    with pytest.raises(KeyError):
        read_document(t.factory, t.root + "/absent", "record")
    assert list_collections(t.factory, t.root + "/absent") == []
    write_document(t.factory, t.root, "record", {"long": "x" * 3000})
    t.avus[t.root] = [a for a in t.avus[t.root] if a.units != "1"]
    with pytest.raises(ObjectStoreError, match="Invalid iRODS catalog document"):
        read_document(t.factory, t.root, "record")


def test_read_through_cache_mutation_provenance_and_scoped_deletion(remote):
    t, store = remote, mapping(remote)
    store["a/0"] = b"first"
    store["b/0"] = b"second"
    assert "a/0" in store
    assert list(store) == ["a/0", "b/0"]
    assert store.getsize("a/0") == 5
    assert store.getsize("b") == 6
    assert t.state.downloads == 0
    assert store["a/0"] == b"first"
    assert store["a/0"] == b"first"
    receipt = store.provenance("a/0")
    assert receipt["source"]["logical_path"] == t.root + "/v1/a/0"
    assert receipt["source"]["checksum"] == checksum(b"first")
    assert t.state.downloads == 1
    cached = store.local_path("a/0")
    cached.write_bytes(b"corrupt")
    assert store["a/0"] == b"first"
    assert t.state.downloads == 2
    store["a/0"] = b"updated"
    assert store["a/0"] == b"updated"
    assert store.local_path("a/0") != cached
    store.rmdir("a")
    assert list(store) == ["b/0"]
    assert cached.exists()
    with pytest.raises(KeyError):
        store["a/0"]
    assert list_collections(t.factory, t.root) == [t.root + "/v1"]
    del store["b/0"]
    with pytest.raises(KeyError):
        del store["b/0"]
    store.rmdir()
    assert len(store) == 0
    assert t.state.opened == t.state.closed


def test_default_reads_do_not_create_local_files(remote, monkeypatch):
    store = IRODSZarrMapping(remote.factory, remote.root + "/v1")
    store["a"] = b"verified"

    def unexpected_local_file(*args, **kwargs):
        pytest.fail("An uncached read must not create cache or temporary files.")

    monkeypatch.setattr(Path, "mkdir", unexpected_local_file)
    monkeypatch.setattr(
        "openghg.objectstore._irods_storage.tempfile.TemporaryDirectory", unexpected_local_file
    )
    assert store["a"] == b"verified"
    assert store["a"] == b"verified"
    assert remote.state.downloads == 2
    remote.session.data_objects.replicate.assert_not_called()
    assert not remote.cache.exists()
    for operation in (store.local_path, store.provenance):
        with pytest.raises(ValueError, match="caching is disabled"):
            operation("a")
    del remote.payloads[remote.root + "/v1/a"]
    with pytest.raises(KeyError):
        store["a"]
    assert remote.state.opened == remote.state.closed


@pytest.mark.parametrize("cached", [False, True])
def test_strict_resource_reads_use_selected_replica_and_cache_receipt(remote, cached):
    collection = remote.root + "/v1"
    IRODSZarrMapping(remote.factory, collection)["a"] = b"verified"
    path = collection + "/a"
    remote.replicas[path] = [
        SimpleNamespace(
            number=3,
            status="1",
            resource_name="mirrorResc",
            resc_hier="localTier;mirrorResc",
            checksum=checksum(b"verified"),
            size=8,
        )
    ]
    store = IRODSZarrMapping(
        remote.factory,
        collection,
        remote.cache if cached else None,
        read_only=True,
        read_resource="localTier",
    )
    assert store["a"] == b"verified"
    if cached:
        downloads = [call for call in remote.session.data_objects.get.call_args_list if len(call.args) > 1]
        assert downloads[-1].kwargs["replNum"] == "3"
        receipt = store.provenance("a")
        assert receipt["source"]["resource"] == "mirrorResc"
        assert receipt["source"]["replica_number"] == 3
        assert store["a"] == b"verified"
        assert remote.state.downloads == 1
    else:
        assert remote.session.data_objects.open.call_args.kwargs["replNum"] == "3"
        assert not remote.cache.exists()
    remote.session.data_objects.replicate.assert_not_called()


@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize("existing", [False, True])
def test_missing_or_stale_resource_fails_without_opt_in_and_can_fill_on_read(remote, cached, existing):
    collection = remote.root + "/v1"
    IRODSZarrMapping(remote.factory, collection)["a"] = b"verified"
    path = collection + "/a"
    if existing:
        remote.replicas[path] = [
            SimpleNamespace(
                number=1,
                status="0",
                resource_name="mirrorResc",
                resc_hier="mirrorResc",
                checksum=checksum(b"obsolete"),
                size=8,
            )
        ]
    strict = IRODSZarrMapping(
        remote.factory,
        collection,
        remote.cache if cached else None,
        read_only=True,
        read_resource="mirrorResc",
    )
    with pytest.raises(ObjectStoreError, match="no good replica.*mirrorResc"):
        strict["a"]
    assert remote.state.downloads == 0
    remote.session.data_objects.replicate.assert_not_called()
    mirror = IRODSZarrMapping(
        remote.factory,
        collection,
        remote.cache if cached else None,
        read_only=True,
        read_resource="mirrorResc",
        replicate_on_read=True,
    )
    original_id = remote.ids[path]
    assert mirror["a"] == b"verified"
    assert mirror["a"] == b"verified"
    assert remote.ids[path] == original_id
    remote.session.data_objects.replicate.assert_called_once_with(
        path,
        destRescName="mirrorResc",
        updateRepl="",
        verifyChksum="",
    )
    assert remote.replicas[path][0].status == "1"
    assert remote.replicas[path][0].checksum == checksum(b"verified")
    transfer = (
        [call for call in remote.session.data_objects.get.call_args_list if len(call.args) > 1][-1]
        if cached
        else remote.session.data_objects.open.call_args
    )
    assert transfer.kwargs["replNum"] == "1"
    assert remote.cache.exists() is cached
    assert remote.session.numThreads == 4
    assert remote.state.opened == remote.state.closed


@pytest.mark.parametrize("cached", [False, True])
def test_demand_replication_denial_does_not_fall_back_to_source_or_cache(remote, cached):
    collection = remote.root + "/v1"
    IRODSZarrMapping(remote.factory, collection)["a"] = b"verified"
    mirror = IRODSZarrMapping(
        remote.factory,
        collection,
        remote.cache if cached else None,
        read_only=True,
        read_resource="mirrorResc",
        replicate_on_read=True,
    )
    assert mirror["a"] == b"verified"
    downloads = remote.state.downloads
    remote.replicas[collection + "/a"][0].status = "0"
    remote.session.data_objects.replicate.side_effect = PermissionError("server denied replication")
    with pytest.raises(PermissionError, match="server denied"):
        mirror["a"]
    assert remote.state.downloads == downloads


@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize("code,error", [(-121000, PermissionError), (-999999, ObjectStoreError)])
def test_unmapped_replication_errors_cannot_become_zarr_fill_values(remote, cached, code, error):
    collection = remote.root + "/v1"
    source = IRODSKVStore(IRODSZarrMapping(remote.factory, collection))
    zarr.array([7.0, 8.0], store=source, chunks=1, fill_value=-999.0)
    mirror = IRODSKVStore(
        IRODSZarrMapping(
            remote.factory,
            collection,
            remote.cache if cached else None,
            read_only=True,
            read_resource="mirrorResc",
            replicate_on_read=True,
        )
    )
    array = zarr.open_array(mirror, mode="r")
    downloads = remote.state.downloads
    remote.session.data_objects.replicate.side_effect = KeyError(code)
    with pytest.raises(error, match=str(code)) as raised:
        array[:]
    assert isinstance(raised.value.__cause__, KeyError)
    assert raised.value.__cause__.args == (code,)
    assert remote.state.downloads == downloads
    assert remote.state.opened == remote.state.closed


@pytest.mark.parametrize("args", [("missing setting",), (0,), (42,), (-121000, "context")])
def test_replication_preserves_nonserver_key_errors(remote, args):
    collection = remote.root + "/v1"
    IRODSZarrMapping(remote.factory, collection)["a"] = b"verified"
    mirror = IRODSZarrMapping(
        remote.factory,
        collection,
        read_resource="mirrorResc",
        replicate_on_read=True,
    )
    error = KeyError(*args)
    remote.session.data_objects.replicate.side_effect = error
    with pytest.raises(KeyError) as raised:
        mirror["a"]
    assert raised.value is error


@pytest.mark.parametrize("failure", ["identity", "stale", "checksum"])
def test_failed_replication_verification_prevents_read(remote, failure):
    collection = remote.root + "/v1"
    IRODSZarrMapping(remote.factory, collection)["a"] = b"verified"
    path = collection + "/a"
    replicate = remote.session.data_objects.replicate.side_effect

    def broken_replication(*args, **kwargs):
        replicate(*args, **kwargs)
        if failure == "identity":
            remote.ids[path] += 1
        elif failure == "stale":
            remote.replicas[path][0].status = "0"
        else:
            remote.replicas[path][0].checksum = checksum(b"incorrect")

    remote.session.data_objects.replicate.side_effect = broken_replication
    mirror = IRODSZarrMapping(
        remote.factory,
        collection,
        read_resource="mirrorResc",
        replicate_on_read=True,
    )
    with pytest.raises(ObjectStoreError):
        mirror["a"]
    assert remote.state.downloads == 0


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
def test_invalid_mirror_options_fail_before_remote_access(remote, options):
    opened = remote.state.opened
    with pytest.raises(ValueError):
        IRODSZarrMapping(remote.factory, remote.root, **options)
    assert remote.state.opened == opened


@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize("failure", ["corrupt", "changed"])
def test_failed_download_does_not_publish_cache(remote, failure, cached):
    store = IRODSZarrMapping(remote.factory, remote.root + "/v1", remote.cache if cached else None)
    store["a"] = b"valid"
    setattr(remote.state, failure, True)
    with pytest.raises(ObjectStoreError):
        store["a"]
    assert not list(remote.cache.rglob("data"))
    assert not list(remote.cache.rglob("provenance.json"))


@pytest.mark.parametrize("key", ["../outside", "/outside", "a/../b", "a//b", "a\\b", "a'b", "a\x00b"])
def test_key_paths_are_confined(remote, key):
    store = mapping(remote)
    with pytest.raises(ValueError):
        store[key] = b"bad"
    assert not remote.payloads


def test_read_only_blocks_all_mutations(remote):
    store = mapping(remote, read_only=True)
    wrapper = IRODSKVStore(store)
    assert not wrapper.is_writeable()
    assert not wrapper.is_erasable()
    for operation in (lambda: store.__setitem__("a", b"x"), lambda: store.__delitem__("a"), store.rmdir):
        with pytest.raises(PermissionError, match="read-only"):
            operation()
    assert not remote.payloads


def test_writer_context_guard_blocks_retained_mutable_mapping(remote):
    guard = Mock()
    store = mapping(remote, write_guard=guard)
    store["a"] = b"original"
    guard.side_effect = ObjectStoreError("Writer context is closed")
    for operation in (lambda: store.__setitem__("a", b"new"), lambda: store.__delitem__("a"), store.rmdir):
        with pytest.raises(ObjectStoreError, match="Writer context is closed"):
            operation()
    assert store["a"] == b"original"


@pytest.mark.parametrize("special", [None, "root", "ancestor"])
def test_root_validation_checks_ordinary_ancestors(remote, special):
    from irods.models import Collection, ModelBase
    from irods.results import ResultSet

    def query(name, collection_type):
        assert name == Collection.name
        assert collection_type.icat_id == 510

        def select(criterion):
            assert remote.root in criterion.value
            assert "/testZone/home/test" in criterion.value
            paths = {path: "0" for path in criterion.value}
            paths["/testZone/home/test"] = ""
            if special == "root":
                paths[remote.root] = "linkPoint"
            if special == "ancestor":
                paths["/testZone/home/test"] = "mountPoint"
            return [{name: path, collection_type: kind} for path, kind in paths.items()]

        return SimpleNamespace(filter=select)

    remote.session.query.side_effect = query
    with remote.factory() as session:
        if special is None:
            assert validate_ordinary_collection(session, remote.root) == remote.root
        else:
            with pytest.raises(ObjectStoreError, match="requires ordinary collections"):
                validate_ordinary_collection(session, remote.root)
    # Result parsing also needs the custom column in PRC's already populated registry.
    column, value = ResultSet._format_attribute(510, "0")
    assert column == ModelBase.columns()[510]
    assert value == "0"


def test_root_validation_rejects_unknown_collection_type(remote):
    remote.session.query.return_value.filter.return_value = []
    with remote.factory() as session, pytest.raises(ObjectStoreError, match="could not be verified"):
        validate_ordinary_collection(session, remote.root)


def test_root_validation_rejects_hidden_ancestor(remote):
    def query(name, collection_type):
        return SimpleNamespace(filter=lambda criterion: [{name: remote.root, collection_type: "0"}])

    remote.session.query.side_effect = query
    with remote.factory() as session, pytest.raises(ObjectStoreError, match="Read access.*ancestors"):
        validate_ordinary_collection(session, remote.root)


def test_zarr_adapter_sizes_and_removes_without_reading_payloads(remote):
    store = IRODSKVStore(mapping(remote))
    store["array/0"] = b"first"
    store["array/1"] = b"second"
    assert store.getsize("array") == 11
    assert store.getsize("array/0") == 5
    store.rmdir("array")
    assert len(store) == 0
    assert remote.state.downloads == 0
    remote.session.collections.remove.assert_called_once_with(
        remote.root + "/v1/array", recurse=True, force=False
    )


def test_xarray_lazy_chunks_reopen_sessions_and_copy_versions(remote):
    store = mapping(remote)
    original = xr.Dataset({"mf": ("time", np.arange(8.0))}, coords={"time": np.arange(8)})
    original.to_zarr(
        zarr.storage.KVStore(store),
        encoding={"mf": {"chunks": (2,)}, "time": {"chunks": (2,)}},
        consolidated=True,
    )
    remote.state.downloads = 0
    lazy = xr.open_zarr(zarr.storage.KVStore(mapping(remote, read_only=True)), consolidated=True)
    assert remote.state.active == 0
    initial_downloads = remote.state.downloads
    xr.testing.assert_equal(lazy.isel(time=slice(0, 2)).load(), original.isel(time=slice(0, 2)))
    assert remote.state.downloads == initial_downloads + 1
    assert remote.state.opened == remote.state.closed
    second = IRODSZarrMapping(remote.factory, remote.root + "/v2", remote.cache)
    zarr.copy_store(zarr.storage.KVStore(store), zarr.storage.KVStore(second))
    xr.testing.assert_equal(xr.open_zarr(zarr.storage.KVStore(second), consolidated=True).load(), original)
    assert list_collections(remote.factory, remote.root) == [remote.root + "/v1", remote.root + "/v2"]


def test_existing_versioned_store_append_update_and_delete(remote):
    def factory(version):
        return IRODSKVStore(IRODSZarrMapping(remote.factory, remote.root + "/" + version, remote.cache))

    store = VersionedZarrStore(factory)
    original = xr.Dataset({"mf": ("time", [1.0, 2.0])}, coords={"time": [0, 1]})
    store.create_version("v1", checkout=True)
    store.insert(original)
    store.copy_to_version("v2")
    store.checkout_version("v2")
    store.insert(xr.Dataset({"mf": ("time", [3.0])}, coords={"time": [2]}))
    store.update(xr.Dataset({"mf": ("time", [20.0])}, coords={"time": [1]}))
    expected = xr.Dataset({"mf": ("time", [1.0, 20.0, 3.0])}, coords={"time": [0, 1, 2]})
    xr.testing.assert_equal(store.get().load(), expected)
    downloads = remote.state.downloads
    assert store.bytes_stored() > 0
    assert remote.state.downloads == downloads
    store.checkout_version("v1")
    xr.testing.assert_equal(store.get().load(), original)
    store.delete_version("v2")
    assert list_collections(remote.factory, remote.root) == [remote.root + "/v1"]
