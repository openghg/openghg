"""Opt-in acceptance tests against a real, caller-supplied iRODS deployment.

Set IRODS_ENVIRONMENT_FILE and OPENGHG_IRODS_TEST_COLLECTION to enable these
tests. Each test creates and removes its own UUID-named child collection.
OPENGHG_IRODS_TEST_RESOURCE optionally names a second resource for replication.
"""

from copy import deepcopy
import os
from types import SimpleNamespace
from uuid import uuid4

import numpy as np
import pytest
import xarray as xr

pytest.importorskip("irods")

from helpers import get_surface_datapath
from irods.session import iRODSSession

from openghg.dataobjects import data_manager
from openghg.objectstore import IRODSObjectStore, integrity_check, open_object_store
from openghg.objectstore._irods_storage import document_attribute
from openghg.retrieve import get_obs_surface, search_surface
from openghg.standardise import standardise_surface
from openghg.store import create_custom_config, get_metakeys, write_metakeys
from openghg.types import DataOverlapError, ObjectStoreError, ZarrStoreError
from openghg.util import check_if_need_new_version


@pytest.fixture
def live_irods(tmp_path, monkeypatch):
    root = os.environ.get("OPENGHG_IRODS_TEST_COLLECTION")
    environment_file = os.environ.get("IRODS_ENVIRONMENT_FILE")
    if not root or not environment_file:
        pytest.skip("Set IRODS_ENVIRONMENT_FILE and OPENGHG_IRODS_TEST_COLLECTION for live tests.")

    collection = root.rstrip("/") + "/openghg-test-" + str(uuid4())
    cache = tmp_path / "irods-cache"
    configuration = {
        "user_id": str(uuid4()),
        "config_version": "2",
        "object_store": {
            "user": {
                "path": collection,
                "permissions": "rw",
                "factory": "openghg.objectstore._irods:irods_object_store",
                "options": {"environment_file": environment_file, "cache_dir": str(cache)},
            }
        },
    }
    monkeypatch.setattr("openghg.objectstore._local_store.read_local_config", lambda: configuration)
    monkeypatch.setattr("openghg.util._user.read_local_config", lambda: configuration)
    # The normal test suite substitutes defaults here; exercise actual backend documents.
    monkeypatch.setattr("openghg.store.base._base.get_metakeys", get_metakeys)
    monkeypatch.delenv("OPENGHG_TUT_STORE", raising=False)

    with iRODSSession(irods_env_file=environment_file) as session:
        session.collections.get(root.rstrip("/"))
        session.collections.create(collection, recurse=False)
        try:
            yield SimpleNamespace(session=session, collection=collection, cache=cache, config=configuration)
        finally:
            session.collections.remove(collection, recurse=True, force=True)


def test_live_irods_round_trip(live_irods):
    """Normal configured workflows preserve versions, management, and replica identity."""
    live = live_irods
    create_custom_config(live.collection)
    metakeys = deepcopy(get_metakeys(live.collection))
    metakeys["surface"]["optional"]["backend_note"] = {"type": ["str"]}
    write_metakeys(live.collection, metakeys)
    assert get_metakeys(live.collection) == metakeys

    # One public call processes both files through the same reusable BaseStore.
    files = [
        get_surface_datapath("bsd.picarro.1minute.248m.min.dat", source_format="CRDS"),
        get_surface_datapath("bsd.picarro.1minute.248m.co2_mod.dat", source_format="CRDS"),
    ]
    stored = standardise_surface(
        store="user",
        filepath=files,
        site="bsd",
        network="decc",
        source_format="CRDS",
        sort_files=False,
        if_exists="new",
        save_current="yes",
        info_metadata={"backend_note": "catalog backed"},
    )
    assert len({item["file"] for item in stored}) == 2
    selected = dict(store="user", site="bsd", species="co2", inlet="248m")
    results = search_surface(**selected)
    assert len(results.metadata) == 1
    uuid, metadata = next(iter(results.metadata.items()))
    assert metadata["object_store"] == live.collection
    assert metadata["backend_note"] == "catalog backed"
    assert metadata["latest_version"] == "v2"
    assert set(metadata["versions"]) == {"v1", "v2"}

    # Retrieval closes its ObjectStore before these lazy arrays are computed.
    original = get_obs_surface(**selected, version="v1")
    latest = get_obs_surface(**selected)
    assert original.data.mf.max().compute().item() < 425
    assert latest.data.mf.min().compute().item() > 9000
    with open_object_store(live.collection, "surface", mode="r") as reader:
        deferred = reader.get_datasource(uuid).get_data()
    # The public retrieval layer renames the raw species variable to "mf".
    np.testing.assert_equal(deferred.co2.compute().values, latest.data.mf.compute().values)
    np.testing.assert_equal(deferred.time.values, latest.data.time.values)
    assert integrity_check() is None

    manager = data_manager(data_type="surface", **selected)
    manager.update_metadata(uuid, to_update={"comment": "metadata edited through catalog"})
    manager.update_attributes(uuid, to_update={"backend_attribute": "edited"}, data_vars="co2")
    changed = get_obs_surface(**selected)
    assert changed.data.attrs["backend_attribute"] == "edited"
    assert changed.data.mf.attrs["backend_attribute"] == "edited"
    assert search_surface(**selected).metadata[uuid]["comment"] == "metadata edited through catalog"
    assert "backend_attribute" not in get_obs_surface(**selected, version="v1").data.attrs

    writer = IRODSObjectStore(live.session, live.collection, live.cache, mode="rw")
    with writer:
        contender = IRODSObjectStore(live.session, live.collection, live.cache, mode="rw")
        with pytest.raises(ObjectStoreError, match="writer lock"):
            with contender:
                pass
        assert live.session.collections.exists(writer.lock_path)
        target_resource = os.environ.get("OPENGHG_IRODS_TEST_RESOURCE")
        if target_resource:
            datasource = writer.get_datasource(uuid)
            mapping = datasource.mapping()
            key = next(
                k for k in mapping if k.startswith("co2/") and not k.rsplit("/", 1)[-1].startswith(".")
            )
            receipt = mapping.provenance(key)
            source = receipt["source"]
            original_id = source["data_id"]
            writer.replicate(uuid, target_resource)
            replicated = live.session.data_objects.get(source["logical_path"])
            good_replicas = [r for r in replicated.replicas if str(r.status) == "1"]
            assert str(replicated.id) == original_id
            assert len(good_replicas) >= 2
            assert any(r.resource_name == target_resource for r in good_replicas)
            assert {r.checksum for r in good_replicas} == {source["checksum"]}
            assert mapping.provenance(key)["source"]["data_id"] == original_id
    assert not live.session.collections.exists(writer.lock_path)
    writer.close()  # Repeated close must not affect the borrowed authenticated session.
    live.session.collections.get(live.collection)

    readonly = IRODSObjectStore(live.session, live.collection, live.cache, mode="r")
    for mutate in (
        lambda: readonly.create({}, xr.Dataset()),
        lambda: readonly.update(uuid, metadata={"comment": "forbidden"}),
        lambda: readonly.delete(uuid),
        lambda: readonly.write_document("forbidden.json", {}),
        lambda: readonly.get_datasource(uuid).update_attributes(to_update={"forbidden": True}),
    ):
        with pytest.raises(PermissionError):
            mutate()
    live.config["object_store"]["user"]["permissions"] = "r"
    with pytest.raises(ObjectStoreError, match="does not allow writes"):
        with open_object_store(live.collection, "surface", mode="rw"):
            pass
    assert search_surface(**selected)
    live.config["object_store"]["user"]["permissions"] = "rw"

    manager.delete_datasource(uuid)
    assert not search_surface(**selected)
    assert live.session.collections.exists(writer.metastore.path(uuid))
    assert writer.metastore.publication(uuid)["record"] is None
    assert integrity_check() is None


def test_live_irods_update_policies_and_large_metadata(live_irods):
    """Direct stores use normal overlap policies and catalog documents larger than one AVU."""
    live = live_irods
    times = np.arange("2020-01-01T00:00", "2020-01-01T00:04", dtype="datetime64[m]").astype("datetime64[ns]")
    first = xr.Dataset({"mf": ("time", [1.0, 2.0])}, coords={"time": times[:2]})
    appended = xr.Dataset({"mf": ("time", [3.0, 4.0])}, coords={"time": times[2:]})
    replacement = xr.Dataset({"mf": ("time", [20.0, 30.0])}, coords={"time": times[1:3]})
    combined = xr.Dataset({"mf": ("time", [1.0, 20.0, 30.0, 4.0])}, coords={"time": times})
    large_value = "αβ🙂 provenance record with 'quotes' and \\slashes " * 300
    metadata = {"species": "ch4", "data_type": "surface", "history": large_value}
    writer = IRODSObjectStore(live.session, live.collection, live.cache, mode="rw")

    with pytest.raises(ObjectStoreError, match="with statement"):
        writer.create(metadata.copy(), first, period="60s")
    with writer:
        uuid = writer.create(metadata.copy(), first, period="60s")
    assert writer.metastore.record(uuid)["history"] == large_value
    record_avus = live.session.collections.get(writer.metastore.path(uuid)).metadata.get_all(
        document_attribute("publication")
    )
    assert len(record_avus) > 2

    with writer:
        writer.update(
            uuid,
            data=appended,
            if_exists="auto",
            new_version=check_if_need_new_version("auto", "auto"),
        )
    datasource = writer.get_datasource(uuid)
    assert datasource.latest_version == "v1"
    xr.testing.assert_equal(datasource.get_data().compute(), xr.concat([first, appended], dim="time"))
    with pytest.raises(DataOverlapError):
        with writer:
            writer.update(uuid, data=replacement, if_exists="auto", new_version=False)
    assert not live.session.collections.exists(writer.lock_path)
    assert writer.get_datasource(uuid).latest_version == "v1"

    with writer:
        writer.update(
            uuid,
            data=replacement,
            if_exists="combine",
            new_version=check_if_need_new_version("combine", "yes"),
        )
    datasource = writer.get_datasource(uuid)
    assert datasource.latest_version == "v2"
    xr.testing.assert_equal(datasource.get_data("v1").compute(), xr.concat([first, appended], dim="time"))
    xr.testing.assert_equal(datasource.get_data().compute(), combined)

    target_resource = os.environ.get("OPENGHG_IRODS_TEST_RESOURCE")
    if target_resource:
        mapping = datasource.mapping()
        key = next(k for k in mapping if k.startswith("mf/") and not k.rsplit("/", 1)[-1].startswith("."))
        path = mapping.collection + "/" + key
        original_id = live.session.data_objects.get(path).id
        with writer:
            writer.replicate(uuid, target_resource)
            writer.update(
                uuid,
                data=replacement.assign(mf=replacement.mf * 10),
                if_exists="combine",
                new_version=False,
            )
            original_object = live.session.data_objects.get(path)
            assert original_object.id == original_id
            assert all(str(r.status) == "1" for r in original_object.replicas)
            new_path = writer.get_datasource(uuid).mapping().collection + "/" + key
            assert new_path != path
            replacement_id = live.session.data_objects.get(new_path).id
            assert replacement_id != original_id
            writer.replicate(uuid, target_resource)
        refreshed = live.session.data_objects.get(new_path)
        assert refreshed.id == replacement_id
        good_replicas = [r for r in refreshed.replicas if str(r.status) == "1"]
        assert len(good_replicas) >= 2
        assert any(r.resource_name == target_resource for r in good_replicas)
        assert len({(r.checksum, int(r.size)) for r in good_replicas}) == 1
        refreshed_data = writer.get_datasource(uuid).get_data().compute()
        np.testing.assert_equal(refreshed_data.mf.values, [1.0, 200.0, 300.0, 4.0])

    with writer:
        writer.update(
            uuid,
            data=first,
            if_exists="new",
            new_version=check_if_need_new_version("new", "no"),
        )
    datasource = writer.get_datasource(uuid)
    assert datasource.latest_version == "v2"
    assert set(datasource.metadata["versions"]) == {"v1", "v2"}
    xr.testing.assert_equal(datasource.get_data().compute(), first)

    with writer:
        datasource = writer.get_datasource(uuid)
        datasource.delete_version("v1")
        datasource.save()
    datasource = writer.get_datasource(uuid)
    assert set(datasource.metadata["versions"]) == {"v2"}
    with pytest.raises(ZarrStoreError):
        datasource.get_data("v1")
    datasource.integrity_check()
    with writer:
        writer.delete(uuid)
    assert writer.search() == []
    assert live.session.collections.exists(writer.metastore.path(uuid))
    assert writer.metastore.publication(uuid)["record"] is None


def test_live_irods_atomic_publication_and_retained_readers(live_irods, monkeypatch):
    """Actual AVU publication preserves lazy readers and rejects stale saves."""
    from openghg.objectstore import PublicationConflictError, _irods_metastore

    live = live_irods
    writer = IRODSObjectStore(live.session, live.collection, live.cache, mode="rw")
    original = xr.Dataset(
        {"mf": ("time", [1.0, 2.0])},
        coords={"time": np.array(["2020-01-01", "2020-01-02"], dtype="datetime64[ns]")},
    )
    replacement = original.assign(mf=original.mf * 10)
    with writer:
        uuid = writer.create({"species": "ch4", "comment": "before"}, original, period="1D")
    pinned = writer.get_datasource(uuid)
    lazy = pinned.get_data()
    write = _irods_metastore.write_document
    seen = []

    def before_commit(factory, collection, key, value):
        if key == "publication" and value["record"] is not None:
            assert writer.get_datasource(uuid).revision == pinned.revision
            assert writer.search(uuid=uuid)[0]["comment"] == "before"
            xr.testing.assert_equal(writer.get_datasource(uuid).get_data().compute(), original)
            seen.append(True)
        write(factory, collection, key, value)

    monkeypatch.setattr(_irods_metastore, "write_document", before_commit)
    with writer:
        writer.update(uuid, {"comment": "after"}, replacement, if_exists="new", new_version=False)
        with pytest.raises(PublicationConflictError):
            pinned.save()
    assert seen
    monkeypatch.setattr(_irods_metastore, "write_document", write)
    xr.testing.assert_equal(writer.get_datasource(uuid).get_data().compute(), replacement)
    with writer:
        writer.delete(uuid)
    assert writer.search() == []
    xr.testing.assert_equal(lazy.compute(), original)
    xr.testing.assert_equal(pinned.get_data().compute(), original)
