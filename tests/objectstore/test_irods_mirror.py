"""Mirror coverage checks and opt-in acceptance against native iRODS resources.

Live checks need the normal iRODS test environment plus SOURCE_RESOURCE and
MIRROR_RESOURCE (prefixed OPENGHG_IRODS_). The Compose lab also requires distinct
resource hosts and supplies a client environment connecting to the consumer.
"""

from contextlib import nullcontext
import json
import os
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pytest
import xarray as xr

pytest.importorskip("irods")
from irods.exception import iRODSException

from test_irods import live_irods as live_irods  # noqa: F401
from test_irods_access import shared_irods as shared_irods  # noqa: F401

from openghg.objectstore import IRODSObjectStore, open_object_store
from openghg.objectstore import irods_mirror
from openghg.objectstore._irods_storage import IRODSZarrMapping, _snapshot
from openghg.types import ObjectStoreError


def test_report_distinguishes_missing_and_unavailable_and_verifies_target(monkeypatch):
    source = SimpleNamespace(status="1", checksum="sum", size=3, resc_hier="source", resource_name="source")
    good = SimpleNamespace(
        status="1", checksum="sum", size=3, resc_hier="parent;mirror", resource_name="mirror"
    )
    stale = SimpleNamespace(
        status="0", checksum="old", size=2, resc_hier="parent;mirror", resource_name="mirror"
    )
    replicas = {"/data/a": [source, good], "/data/b": [source, stale], "/data/c": [source]}
    session = SimpleNamespace(
        resources=SimpleNamespace(get=Mock()),
        data_objects=SimpleNamespace(get=lambda path: SimpleNamespace(replicas=replicas[path])),
    )

    class Mapping:
        collection = "/data"

        def __iter__(self):
            return iter(["a", "b", "c"])

    datasource = SimpleNamespace(
        revision="observed-revision", _store=SimpleNamespace(versions=["v1"]), mapping=lambda _: Mapping()
    )
    store = SimpleNamespace(
        uuids=["one"],
        collection="/store",
        data_type="surface",
        _sessions=lambda: nullcontext(session),
        get_datasource=lambda _: datasource,
    )
    monkeypatch.setattr(irods_mirror, "_snapshot", lambda *_: {"checksum": "sum", "size": 3})
    checked = []

    class Target:
        def __init__(self, factory, collection, *, read_only, read_resource):
            assert collection == "/data" and read_only and read_resource == "mirror"

        def __getitem__(self, key):
            checked.append(key)
            return b"abc"

    monkeypatch.setattr(irods_mirror, "IRODSZarrMapping", Target)
    report = irods_mirror.replica_report(store, "mirror", verify=True)
    record = report["datasources"][0]
    assert not report["complete"]
    assert record["revision"] == "observed-revision"
    assert (record["good_objects"], record["unavailable_objects"], record["missing_objects"]) == (1, 1, 1)
    assert record["bytes"] == 9 and record["payloads_verified"] == 1
    assert checked == ["a"]
    for values in replicas.values():
        values[:] = [source, good]
    assert irods_mirror.replica_report(store, "mirror")["complete"]
    assert checked == ["a"]  # Catalogue-only status performs no payload reads.
    monkeypatch.setattr(Target, "__getitem__", Mock(side_effect=ObjectStoreError("corrupt target")))
    with pytest.raises(ObjectStoreError, match="corrupt target"):
        irods_mirror.replica_report(store, "mirror", verify=True)
    monkeypatch.setattr(Mapping, "__iter__", lambda _: iter(()))
    with pytest.raises(ObjectStoreError, match="No catalogued Zarr objects"):
        irods_mirror.replica_report(store, "mirror")


def test_sync_reports_new_datasources_published_during_copy(monkeypatch):
    store = SimpleNamespace(uuids=["first"])

    def publish_during_copy(uuid, resource):
        assert uuid == "first" and resource == "mirror"
        store.uuids = ["first", "new"]

    store.replicate = Mock(side_effect=publish_during_copy)

    def report_current(store, resource, uuids, *, verify):
        selected = store.uuids if uuids is None else uuids
        return {"complete": "new" not in selected, "selected": selected}

    monkeypatch.setattr(irods_mirror, "replica_report", report_current)
    report = irods_mirror.sync_replicas(store, "mirror")
    assert not report["complete"]
    assert report["selected"] == ["first", "new"]
    store.replicate.assert_called_once_with("first", "mirror")
    # An explicit selection remains bounded to the requested datasource.
    assert irods_mirror.sync_replicas(store, "mirror", ["first"])["complete"]


def _resources(session):
    origin = os.environ.get("OPENGHG_IRODS_SOURCE_RESOURCE")
    mirror = os.environ.get("OPENGHG_IRODS_MIRROR_RESOURCE")
    if not origin or not mirror:
        pytest.skip("Set OPENGHG_IRODS_SOURCE_RESOURCE and OPENGHG_IRODS_MIRROR_RESOURCE.")
    assert origin != mirror
    source_resource = session.resources.get(origin)
    mirror_resource = session.resources.get(mirror)
    if os.environ.get("OPENGHG_IRODS_MIRROR_REQUIRE_DISTINCT_HOSTS") == "1":
        assert source_resource.location != mirror_resource.location
    return origin, mirror


def test_live_registered_resource_mirror(live_irods, capsys):
    """Demand fill preserves IDs, strictly reads the target, and syncs new publications."""
    live = live_irods
    origin, mirror = _resources(live.session)

    def factory():
        return nullcontext(live.session)

    source = IRODSZarrMapping(factory, live.collection + "/transfer-probe", resource=origin)
    # Larger than iRODS's usual parallel-transfer threshold; no portal ports are
    # exposed by the Compose lab. This exercises a real server-to-server transfer.
    payload = np.arange(1_500_000, dtype=np.uint32).tobytes()
    source["large"] = payload
    path = source.collection + "/large"
    original = _snapshot(live.session, path)
    strict = IRODSZarrMapping(factory, source.collection, read_only=True, read_resource=mirror)
    with pytest.raises(ObjectStoreError):
        strict["large"]
    demand = IRODSZarrMapping(
        factory, source.collection, read_only=True, read_resource=mirror, replicate_on_read=True
    )
    assert demand["large"] == payload
    mirrored = _snapshot(live.session, path, mirror)
    assert mirrored["resource"] == mirror
    assert mirrored["replica_number"] != original["replica_number"]
    assert all(mirrored[key] == original[key] for key in ("data_id", "logical_path", "checksum", "size"))
    assert strict["large"] == payload
    count = len(live.session.data_objects.get(path).replicas)
    assert demand["large"] == payload
    assert len(live.session.data_objects.get(path).replicas) == count

    data = xr.Dataset(
        {"mf": ("time", [1.0, 2.0])},
        coords={"time": np.array(["2020-01-01", "2020-01-02"], dtype="datetime64[ns]")},
    )
    writer = IRODSObjectStore(live.session, live.collection, mode="rw", resource=origin)
    with writer:
        uuid = writer.create({"species": "ch4"}, data, period="1D")
    assert not irods_mirror.replica_report(writer, mirror)["complete"]
    options = live.config["object_store"]["user"]["options"]
    options.update(read_resource=mirror, replicate_on_read=True)
    consumer_environment = os.environ.get("OPENGHG_IRODS_MIRROR_ENVIRONMENT_FILE")
    if consumer_environment:
        options["environment_file"] = consumer_environment
    with open_object_store(live.collection, "surface", mode="r") as reader:
        assert reader.search(species="ch4")[0]["uuid"] == uuid
        pinned = reader.get_datasource(uuid)
        lazy = pinned.get_data()
    xr.testing.assert_equal(lazy.load(), data)
    report = irods_mirror.replica_report(writer, mirror, verify=True)
    assert report["datasources"][0]["payloads_verified"] > 0
    # Consolidated Zarr reads do not request every separate metadata object.
    # Demand fill covers requested keys; sync completes the whole generation.
    assert not report["complete"]
    status_args = [
        "--environment-file",
        os.environ["IRODS_ENVIRONMENT_FILE"],
        "status",
        live.collection,
        "--resource",
        mirror,
        "--data-type",
        "surface",
    ]
    assert irods_mirror.main(status_args) == 2
    assert not json.loads(capsys.readouterr().out)["complete"]
    sync_args = [*status_args]
    sync_args[2] = "sync"
    assert irods_mirror.main([*sync_args, "--verify"]) == 0
    assert json.loads(capsys.readouterr().out)["complete"]
    assert irods_mirror.main(status_args) == 0
    assert json.loads(capsys.readouterr().out)["complete"]
    replacement = data.assign(mf=data.mf * 10)
    with writer:
        writer.update(uuid, data=replacement, if_exists="new", new_version=False)
        assert not irods_mirror.replica_report(writer, mirror)["complete"]
        assert irods_mirror.sync_replicas(writer, mirror, verify=True)["complete"]
        assert irods_mirror.sync_replicas(writer, mirror)["complete"]
    with open_object_store(live.collection, "surface", mode="r") as reader:
        xr.testing.assert_equal(reader.get_datasource(uuid).get_data().load(), replacement)
    xr.testing.assert_equal(pinned.get_data().load(), data)
    with writer:
        writer.delete(uuid)
    assert irods_mirror.replica_report(writer, mirror)["datasources"] == []
    xr.testing.assert_equal(pinned.get_data().load(), data)
    assert not live.cache.exists()


def test_live_reader_needs_prepopulated_replicas(shared_irods):
    """Native reader ACLs reject demand replication but allow prepopulated copies."""
    live = shared_irods
    origin, mirror = _resources(live.admin)
    data = xr.Dataset(
        {"mf": ("time", [1.0, 2.0])},
        coords={"time": np.array(["2020-01-01", "2020-01-02"], dtype="datetime64[ns]")},
    )
    writer = IRODSObjectStore(live.admin, live.collection, mode="rw", resource=origin)
    with writer:
        uuid = writer.create({"species": "ch4"}, data, period="1D")
    with live.connect("reader") as session:
        reader = IRODSObjectStore(session, live.collection, read_resource=mirror, replicate_on_read=True)
        assert reader.search(species="ch4")
        with pytest.raises((iRODSException, PermissionError)):
            reader.get_datasource(uuid).get_data().load()
    assert irods_mirror.replica_report(writer, mirror)["datasources"][0]["good_objects"] == 0
    with writer:
        assert irods_mirror.sync_replicas(writer, mirror, verify=True)["complete"]
    with live.connect("reader") as session:
        reader = IRODSObjectStore(session, live.collection, read_resource=mirror)
        xr.testing.assert_equal(reader.get_datasource(uuid).get_data().load(), data)
