"""Administration is explicit, repeatable, and independent of Unix memberships."""

from contextlib import nullcontext
from copy import deepcopy
from pathlib import PurePosixPath
from types import SimpleNamespace
from unittest.mock import Mock
import warnings

import pytest

pytest.importorskip("irods")
from irods.access import iRODSAccess
from irods.exception import CollectionDoesNotExist, GroupDoesNotExist, UserDoesNotExist

from openghg.objectstore import irods_admin as admin


@pytest.fixture
def catalog(monkeypatch):
    users = {
        name: SimpleNamespace(name=name, zone="zone", type=kind)
        for name, kind in (("admin", "rodsadmin"), ("alice", "rodsuser"), ("bob", "rodsuser"))
    }
    collections, acls, groups, documents = {}, {}, {}, {}
    changes = []

    def get_user(name, user_zone="zone"):
        if name not in users:
            raise UserDoesNotExist()
        return users[name]

    def create_user(name, kind, **kwargs):
        users[name] = SimpleNamespace(name=name, zone="zone", type=kind)
        changes.append(("user", name))

    def get_collection(path):
        if path not in collections:
            raise CollectionDoesNotExist()
        return collections[path]

    def create_collection(path, **kwargs):
        for entry in (PurePosixPath(path), *PurePosixPath(path).parents):
            name = str(entry)
            collections.setdefault(
                name,
                SimpleNamespace(
                    path=name,
                    inheritance=False,
                    subcollections=[],
                    data_objects=[],
                    metadata=SimpleNamespace(items=lambda: []),
                ),
            )
            acls.setdefault(name, [])
        changes.append(("collection", path))
        return collections[path]

    def get_group(name):
        if name not in groups:
            raise GroupDoesNotExist()
        return SimpleNamespace(name=name)

    def create_group(name):
        create_user(name, "rodsgroup")
        groups[name] = set()

    def add_member(group, name, **kwargs):
        groups[group].add(name)
        changes.append(("add", group, name))

    def remove_member(group, name, **kwargs):
        groups[group].remove(name)
        changes.append(("remove", group, name))

    def set_acl(acl, recursive=False, admin=False):
        assert admin is True
        changes.append(("acl", acl.path, acl.access_name, acl.user_name, recursive))
        targets = [p for p in collections if p == acl.path or (recursive and p.startswith(acl.path + "/"))]
        for path in targets:
            if acl.access_name == "inherit":
                collections[path].inheritance = True
            else:
                copied = acl.copy()
                copied.path = path
                copied.user_type = users[acl.user_name].type
                acls[path] = [a for a in acls[path] if a.user_name != acl.user_name] + [copied]

    def read_document(factory, collection, key):
        if (collection, key) not in documents:
            raise KeyError(key)
        return deepcopy(documents[collection, key])

    def write_document(factory, collection, key, value):
        documents[collection, key] = deepcopy(value)

    session = SimpleNamespace(
        username="admin",
        zone="zone",
        users=SimpleNamespace(get=get_user, create=create_user, modify=Mock()),
        groups=SimpleNamespace(
            get=get_group,
            create=create_group,
            getmembers=lambda group: [users[n] for n in sorted(groups[group])],
            addmember=add_member,
            removemember=remove_member,
        ),
        collections=SimpleNamespace(
            get=get_collection, create=create_collection, exists=lambda path: path in collections
        ),
        acls=SimpleNamespace(get=lambda collection: list(acls[collection.path]), set=set_acl),
    )
    create_collection("/zone/home/admin")
    changes.clear()
    monkeypatch.setattr(admin, "validate_ordinary_collection", lambda session, path: path)
    monkeypatch.setattr(admin, "read_document", read_document)
    monkeypatch.setattr(admin, "write_document", write_document)
    monkeypatch.setattr("irods.session.iRODSSession", lambda **kwargs: nullcontext(session))
    return SimpleNamespace(
        session=session,
        users=users,
        collections=collections,
        acls=acls,
        groups=groups,
        documents=documents,
        changes=changes,
        root="/zone/home/admin/store",
    )


def test_setup_is_repeatable_and_grants_only_scoped_access(catalog):
    c = catalog
    c.session.groups.create("readers")
    c.acls["/zone"] = [iRODSAccess("own", "/zone", "readers", "zone", "rodsgroup")]
    c.changes.clear()
    for _ in range(2):
        report = admin.setup_store(
            c.session, c.root, "readers", "writers", readers=["alice"], writers=["bob"]
        )
    assert report["inheritance"] is True
    assert report["groups"] == {"readers": ["alice#zone"], "writers": ["bob#zone"]}
    assert {(a["name"], a["access"]) for a in report["acls"]} == {
        ("readers", "read"),
        ("writers", "own"),
    }
    assert c.changes.count(("collection", c.root)) == 1
    assert c.changes.count(("user", "writers")) == 1
    assert c.changes.count(("add", "readers", "alice")) == 1
    assert c.changes.count(("add", "writers", "bob")) == 1
    assert c.acls["/zone"][0].access_name == "own"  # Preserve stronger ancestor grants.
    for change in c.changes:
        if change[0] == "acl" and change[1] != c.root:
            assert change[2] == "read" and change[-1] is False
    assert any("ACL management and delegation" in note for note in report["notes"])
    assert any("raw server vault" in note for note in report["notes"])


def test_existing_nonempty_collection_needs_explicit_adoption(catalog):
    c = catalog
    c.session.collections.create(c.root)
    child = c.session.collections.create(c.root + "/existing")
    c.collections[c.root].subcollections.append(child)
    c.changes.clear()
    with pytest.raises(ValueError, match="adopt-existing"):
        admin.setup_store(c.session, c.root, "readers", "writers")
    assert c.changes == []
    admin.setup_store(c.session, c.root, "readers", "writers", adopt_existing=True)
    assert child.inheritance is True
    assert {(a.user_name, a.access_name) for a in c.acls[child.path]} == {
        ("readers", "read"),
        ("writers", "own"),
    }
    with pytest.raises(ValueError, match="different access setup"):
        admin.setup_store(c.session, c.root, "other_readers", "writers", adopt_existing=True)


@pytest.mark.parametrize(
    "readers,writers,members",
    [
        ("public", "writers", []),
        ("same", "same", []),
        ("alice", "writers", []),
        ("readers", "writers", ["missing"]),
        ("readers", "writers", ["admin"]),
    ],
)
def test_invalid_principals_make_no_changes(catalog, readers, writers, members):
    with pytest.raises((ValueError, UserDoesNotExist)):
        admin.setup_store(catalog.session, catalog.root, readers, writers, readers=members)
    assert catalog.changes == []


def test_membership_removal_is_idempotent_and_does_not_claim_full_revocation(catalog):
    c = catalog
    admin.setup_store(c.session, c.root, "readers", "writers", writers=["bob"])
    before = deepcopy(c.acls)
    assert admin.set_membership(c.session, "writers", "bob", add=False)
    assert not admin.set_membership(c.session, "writers", "bob", add=False)
    assert c.acls == before
    assert admin.access_report(c.session, c.root)["groups"]["writers"] == []
    assert any("ownership" in note for note in admin.access_report(c.session, c.root)["notes"])


def test_user_creation_preserves_existing_password_and_can_be_reset_explicitly(catalog):
    c = catalog
    assert admin.create_user(c.session, "carol", "test-password")
    assert not admin.create_user(c.session, "carol", "unused-password")
    assert c.session.users.modify.call_count == 1
    c.session.users.modify.assert_called_with("carol", "password", "test-password", user_zone="zone")
    admin.set_password(c.session, "carol", "replacement-password")
    assert c.session.users.modify.call_count == 2
    with pytest.raises(ValueError, match="regular rodsuser"):
        admin.create_user(c.session, "admin", "test-password")
    c.session.username = "alice"
    with pytest.raises(PermissionError, match="rodsadmin"):
        admin.create_user(c.session, "dave", "test-password")


def test_partial_user_provisioning_does_not_leak_password(catalog):
    catalog.session.users.modify.side_effect = RuntimeError("library failure containing test-password")
    with pytest.raises(RuntimeError, match="use set-password") as error:
        admin.create_user(catalog.session, "carol", "test-password")
    assert "test-password" not in str(error.value)
    assert "carol" in catalog.users


def test_cli_password_prompt_and_existing_user_are_safe(catalog, monkeypatch, capsys):
    prompt = Mock(side_effect=["test-password", "test-password"])
    monkeypatch.setattr(admin.getpass, "getpass", prompt)
    assert admin.main(["create-user", "carol"]) == 0
    assert admin.main(["create-user", "carol"]) == 0
    assert prompt.call_count == 2
    assert "test-password" not in capsys.readouterr().out


def test_cli_never_falls_back_to_echoed_password_input(catalog, monkeypatch):
    def insecure_input(prompt):
        warnings.warn("Cannot disable echo", admin.getpass.GetPassWarning)
        return "test-password"

    monkeypatch.setattr(admin.getpass, "getpass", insecure_input)
    with pytest.raises(admin.getpass.GetPassWarning):
        admin.main(["create-user", "carol"])
    assert "carol" not in catalog.users
