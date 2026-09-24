"""Opt-in checks of server-enforced roles using temporary native iRODS users.

These checks require a zone administrator and explicit OPENGHG_IRODS_ADMIN_TESTS=1,
in addition to the environment and dedicated collection used by test_irods.py.
They create and remove users and groups as well as data.
"""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import secrets
from types import SimpleNamespace
from uuid import uuid4

import numpy as np
import pytest
import xarray as xr

pytest.importorskip("irods")

from irods.exception import CollectionDoesNotExist, GroupDoesNotExist, iRODSException
from irods import keywords as kw
from irods.session import iRODSSession

from openghg.objectstore import IRODSObjectStore
from openghg.objectstore._irods_storage import IRODSZarrMapping
from openghg.objectstore.irods_admin import create_user, set_membership, setup_store


@pytest.fixture
def shared_irods(tmp_path):
    environment_file = os.environ.get("IRODS_ENVIRONMENT_FILE")
    root = os.environ.get("OPENGHG_IRODS_TEST_COLLECTION")
    if os.environ.get("OPENGHG_IRODS_ADMIN_TESTS") != "1" or not environment_file or not root:
        pytest.skip("Live access tests require OPENGHG_IRODS_ADMIN_TESTS=1 and administrator configuration.")
    prefix = "ogh_" + uuid4().hex[:12]
    readers, writers = prefix + "_readers", prefix + "_writers"
    usernames = {role: prefix + "_" + role for role in ("reader", "writer", "outsider")}
    passwords = {role: secrets.token_urlsafe(24) for role in usernames}
    collection = root.rstrip("/") + "/" + prefix
    options = json.loads(Path(environment_file).read_text())
    # Each session authenticates as its own user, with no administrator credential file.
    options.pop("irods_authentication_file", None)
    options.pop("password", None)
    options["irods_authentication_scheme"] = "native"

    @contextmanager
    def connect(role):
        config = {**options, "irods_user_name": usernames[role], "password": passwords[role]}
        with iRODSSession(**config) as session:
            assert session.username == usernames[role]
            yield session

    with iRODSSession(irods_env_file=environment_file) as admin:
        created = []
        try:
            for role, username in usernames.items():
                assert create_user(admin, username, passwords[role])
                created.append(username)
            setup_store(
                admin,
                collection,
                readers,
                writers,
                readers=[usernames["reader"]],
                writers=[usernames["writer"]],
            )
            yield SimpleNamespace(
                admin=admin,
                connect=connect,
                collection=collection,
                readers=readers,
                writers=writers,
                usernames=usernames,
                cache=tmp_path,
            )
        finally:
            if admin.collections.exists(collection):
                admin.collections.remove(collection, recurse=True, force=True, **{kw.ADMIN_KW: ""})
            for group in (readers, writers):
                try:
                    admin.groups.get(group)
                except GroupDoesNotExist:
                    continue
                admin.groups.remove(group)
            for username in created:
                # Remove test-user trash too, including objects deleted by the writer.
                trash = f"/{admin.zone}/trash/home/{username}"
                if admin.collections.exists(trash):
                    admin.collections.remove(trash, recurse=True, force=True, **{kw.ADMIN_KW: ""})
                admin.users.remove(username)


def test_live_reader_writer_and_revocation(shared_irods):
    """Readers search/download but cannot mutate; writers update and delete peer-owned data."""
    live = shared_irods
    data = xr.Dataset(
        {"mf": ("time", [1.0, 2.0])},
        coords={"time": np.array(["2020-01-01T00:00", "2020-01-01T01:00"], dtype="datetime64[ns]")},
    )
    # An administrator creates the first object, so writer success cannot be explained by ownership.
    with IRODSObjectStore(live.admin, live.collection, live.cache / "admin", mode="rw") as store:
        uuid = store.create({"species": "ch4", "data_type": "surface"}, data, period="3600s")

    with live.connect("reader") as session:
        store = IRODSObjectStore(session, live.collection, live.cache / "reader")
        assert store.search({"species": "ch4"})[0]["uuid"] == uuid
        datasource = store.get_datasource(uuid)
        xr.testing.assert_equal(datasource.get_data().load(), data)
        mapping = datasource.mapping()
        key = next(k for k in mapping if k.startswith("mf/") and not k.rsplit("/", 1)[-1].startswith("."))
        assert mapping[key]  # Populate a verified local cache before revocation.
        payload = mapping.collection + "/" + key
        with pytest.raises(iRODSException):
            session.collections.get(live.collection).metadata.add("forbidden", "value")
        with pytest.raises(iRODSException):
            with session.data_objects.open(payload, "w"):
                pass
        # Even a client requesting rw cannot evade the server ACL by changing its local config.
        with pytest.raises(iRODSException):
            with IRODSObjectStore(session, live.collection, live.cache / "reader", mode="rw"):
                pass

    with live.connect("outsider") as session:
        with pytest.raises(CollectionDoesNotExist):
            session.collections.get(live.collection)

    with live.connect("writer") as session:
        with IRODSObjectStore(session, live.collection, live.cache / "writer", mode="rw") as store:
            store.update(uuid, metadata={"comment": "writer edited an administrator's record"})
            store.update(uuid, data=data.assign(mf=data.mf * 10), if_exists="new", new_version=True)
            second = store.create({"species": "co2", "data_type": "surface"}, data, period="3600s")
        with live.connect("reader") as reader_session:
            reader = IRODSObjectStore(reader_session, live.collection, live.cache / "reader")
            assert reader.search({"uuid": uuid})[0]["comment"].startswith("writer edited")
            xr.testing.assert_equal(
                reader.get_datasource(uuid).get_data().load(), data.assign(mf=data.mf * 10)
            )
            assert reader.search({"uuid": second})
            retained = reader.get_datasource(second).mapping()
            denied_mapping = IRODSZarrMapping(
                lambda: live.connect("reader"), retained.collection, live.cache / "reader", read_only=True
            )
            denied_key = next(
                k for k in retained if k.startswith("mf/") and not k.rsplit("/", 1)[-1].startswith(".")
            )
            assert denied_mapping[denied_key]
        with store:
            store.delete(uuid)
            assert not store.search({"uuid": uuid})

    # Use a new authenticated connection after membership changes; no stale credential/session assumptions.
    set_membership(live.admin, live.readers, live.usernames["reader"], add=False)
    with live.connect("reader") as session:
        with pytest.raises(CollectionDoesNotExist):
            session.collections.get(live.collection)
    with pytest.raises((KeyError, CollectionDoesNotExist, iRODSException)):
        denied_mapping[denied_key]
