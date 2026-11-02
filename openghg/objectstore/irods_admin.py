"""Set up explicit iRODS users, groups, and ACLs for a shared prototype store.

Run ``python -m openghg.objectstore.irods_admin --help`` for the CLI. These
operations change the iRODS catalogue; they do not change Unix permissions or
expose the server's raw storage vault. Native passwords are entered privately.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from contextlib import nullcontext
import getpass
import json
import os
from pathlib import Path, PurePosixPath
import re
from typing import Any
import warnings

from openghg.objectstore._irods_storage import (
    read_document,
    validate_collection,
    validate_ordinary_collection,
    write_document,
)

_SETUP_DOCUMENT = "access-setup"
_ACCESS_NOTES = [
    "These are root ACLs and current group memberships, not a recursive effective-access audit.",
    "Removing a membership does not remove other grants or ownership of objects that user created.",
    "Writers receive own access throughout the shared tree so they can move shared objects to trash; "
    "this also permits ACL management and delegation, so writer membership requires trusted collaborators.",
    "Access uses authenticated iRODS sessions. Unix group membership grants no catalogue access; "
    "the raw server vault must remain private to the service.",
]


def _name(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,62}", value):
        raise ValueError("Use a simple local-zone account or group name (1–63 ASCII characters).")
    return value


def _group_name(value: str) -> str:
    _name(value)
    if value == "public":
        raise ValueError("Use a dedicated group; this helper does not modify the public group.")
    return value


def _require_admin(session: Any) -> None:
    if session.users.get(session.username, session.zone).type != "rodsadmin":
        raise PermissionError("These setup operations require an authenticated local rodsadmin account.")


def _regular_user(session: Any, username: str) -> Any:
    user = session.users.get(_name(username), session.zone)
    if user.type != "rodsuser":
        raise ValueError(f"Account {username!r} must be a regular rodsuser.")
    return user


def _password(value: str) -> None:
    from irods import MAX_PASSWORD_LENGTH

    if (
        not isinstance(value, str)
        or not 3 <= len(value) <= MAX_PASSWORD_LENGTH
        or not value.isascii()
        or not value.isprintable()
    ):
        raise ValueError(
            f"A native password must contain 3–{MAX_PASSWORD_LENGTH} printable ASCII characters."
        )


def set_password(session: Any, username: str, password: str) -> None:
    """Explicitly reset a regular local user's native password; never persist it locally."""
    _require_admin(session)
    _regular_user(session, username)
    _password(password)
    session.users.modify(username, "password", password, user_zone=session.zone)


def create_user(session: Any, username: str, password: str) -> bool:
    """Create a local rodsuser with a native password, leaving existing users unchanged.

    Returns:
        True if the account was created, False if the regular user already existed.

    Raises:
        RuntimeError: If the account was created but setting its password failed.
            Use ``set-password`` to finish provisioning that account.
    """
    from irods.exception import UserDoesNotExist

    _require_admin(session)
    _name(username)
    _password(password)
    try:
        _regular_user(session, username)
    except UserDoesNotExist:
        pass
    else:
        return False
    session.users.create(username, "rodsuser", user_zone=session.zone)
    try:
        set_password(session, username, password)
    except Exception:
        raise RuntimeError(
            f"Account {username!r} was created, but its password was not set; use set-password."
        ) from None
    return True


def set_membership(session: Any, group: str, username: str, *, add: bool = True) -> bool:
    """Add or remove an explicit regular user; return whether membership changed.

    Removal affects this group only. Direct ACLs, ownership, and other groups
    can still grant access, including write access for a former writer.
    """
    _require_admin(session)
    _group_name(group)
    _regular_user(session, username)
    session.groups.get(group)
    present = any(u.name == username and u.zone == session.zone for u in session.groups.getmembers(group))
    if present == add:
        return False
    operation = session.groups.addmember if add else session.groups.removemember
    operation(group, username, user_zone=session.zone)
    return True


def _ensure_group(session: Any, name: str) -> None:
    from irods.exception import UserDoesNotExist

    try:
        principal = session.users.get(name, session.zone)
    except UserDoesNotExist:
        session.groups.create(name)
    else:
        if principal.type != "rodsgroup":
            raise ValueError(f"Group name {name!r} collides with an existing user account.")


def _setup_record(session: Any, collection: str) -> dict[str, Any] | None:
    try:
        return read_document(lambda: nullcontext(session), collection, _SETUP_DOCUMENT)
    except KeyError:
        return None


def setup_store(
    session: Any,
    collection: str,
    reader_group: str,
    writer_group: str,
    *,
    readers: Sequence[str] = (),
    writers: Sequence[str] = (),
    adopt_existing: bool = False,
) -> dict[str, Any]:
    """Create a shared store, inherit ACLs, and grant explicit reader/writer groups.

    Read access is granted on ancestors nonrecursively so the backend can verify
    their collection types. The store and its existing descendants receive read
    or own access recursively. Writers need own access to move shared objects
    to trash; they can also manage ACLs and delegate access throughout the shared
    tree, so this role is for trusted collaborators. Existing extra ACLs and
    memberships are retained. New memberships must name existing users.

    An unmarked nonempty collection requires ``adopt_existing=True``. A managed
    collection can be set up repeatedly with the same role groups. Changing its
    recorded role groups requires deliberate ACL administration outside this helper.
    """
    from irods.access import iRODSAccess

    _require_admin(session)
    collection = validate_collection(collection)
    path = PurePosixPath(collection)
    if (
        path.parts[1] != session.zone
        or len(path.parts) < 4
        or path.parent == PurePosixPath(f"/{session.zone}/home")
    ):
        raise ValueError(
            "Choose a dedicated collection in the local zone, below a home or project collection."
        )
    _group_name(reader_group)
    _group_name(writer_group)
    if reader_group == writer_group:
        raise ValueError("Reader and writer groups must be distinct.")
    for username in (*readers, *writers):
        _regular_user(session, username)

    record = {"schema_version": 1, "reader_group": reader_group, "writer_group": writer_group}
    exists = session.collections.exists(collection)
    if exists:
        validate_ordinary_collection(session, collection)
        previous = _setup_record(session, collection)
        if previous is not None and previous != record:
            raise ValueError("This collection is already managed with a different access setup.")
        current = session.collections.get(collection)
        if (
            previous is None
            and not adopt_existing
            and (current.subcollections or current.data_objects or current.metadata.items())
        ):
            raise ValueError(
                "Existing collection is not empty; use --adopt-existing after reviewing its ACLs."
            )
    else:
        ancestor = path.parent
        while not session.collections.exists(str(ancestor)):
            if str(ancestor) == "/":
                raise ValueError("An existing parent collection is required in the local zone.")
            ancestor = ancestor.parent
        validate_ordinary_collection(session, str(ancestor))

    # Check both names before creating either group or changing collection ACLs.
    from irods.exception import UserDoesNotExist

    for group in (reader_group, writer_group):
        try:
            principal = session.users.get(group, session.zone)
        except UserDoesNotExist:
            continue
        if principal.type != "rodsgroup":
            raise ValueError(f"Group name {group!r} collides with an existing user account.")
    for group in (reader_group, writer_group):
        _ensure_group(session, group)
    if not exists:
        session.collections.create(collection, recurse=True)

    for ancestor in path.parents:
        if str(ancestor) == "/":
            continue
        acls = session.acls.get(session.collections.get(str(ancestor)))
        for group in (reader_group, writer_group):
            ranks = [
                iRODSAccess.codes.get(
                    {"read": "read_object", "write": "modify_object"}.get(
                        a.access_name, a.access_name.replace(" ", "_")
                    ),
                    0,
                )
                for a in acls
                if a.user_name == group and a.user_zone == session.zone
            ]
            if max(ranks, default=0) < iRODSAccess.codes["read_object"]:
                session.acls.set(iRODSAccess("read", str(ancestor), group, session.zone), admin=True)
    session.acls.set(iRODSAccess("inherit", collection), recursive=True, admin=True)
    for group, access in ((reader_group, "read"), (writer_group, "own")):
        session.acls.set(iRODSAccess(access, collection, group, session.zone), recursive=True, admin=True)
    for group, members in ((reader_group, readers), (writer_group, writers)):
        for username in members:
            set_membership(session, group, username)
    write_document(lambda: nullcontext(session), collection, _SETUP_DOCUMENT, record)
    return access_report(session, collection)


def access_report(session: Any, collection: str) -> dict[str, Any]:
    """Report root ACLs, inheritance, and group rosters without claiming effective access."""
    current = session.collections.get(validate_collection(collection))
    acls = session.acls.get(current)
    groups = sorted({a.user_name for a in acls if a.user_type == "rodsgroup"})
    return {
        "collection": collection,
        "inheritance": bool(current.inheritance),
        "setup": _setup_record(session, collection),
        "acls": sorted(
            [
                {"name": a.user_name, "zone": a.user_zone, "type": a.user_type, "access": a.access_name}
                for a in acls
            ],
            key=lambda a: (a["name"], a["zone"], a["access"]),
        ),
        "groups": {
            group: sorted(f"{u.name}#{u.zone}" for u in session.groups.getmembers(group)) for group in groups
        },
        "notes": list(_ACCESS_NOTES),
    }


def _read_password() -> str:
    with warnings.catch_warnings():
        warnings.simplefilter("error", getpass.GetPassWarning)
        password = getpass.getpass("New native iRODS password: ")
        confirmation = getpass.getpass("Confirm password: ")
    if password != confirmation:
        raise ValueError("The passwords do not match.")
    _password(password)
    return password


def main(argv: Sequence[str] | None = None) -> int:
    """Run the administration CLI using native client authentication settings."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--environment-file",
        default=os.environ.get("IRODS_ENVIRONMENT_FILE", "~/.irods/irods_environment.json"),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    setup = commands.add_parser("setup", help="Create/configure explicit read and write groups for a store")
    setup.add_argument("collection")
    setup.add_argument("--readers", required=True, dest="reader_group")
    setup.add_argument("--writers", required=True, dest="writer_group")
    setup.add_argument("--reader", action="append", default=[], dest="readers")
    setup.add_argument("--writer", action="append", default=[], dest="writers")
    setup.add_argument("--adopt-existing", action="store_true")
    for name in ("create-user", "set-password"):
        commands.add_parser(name, help="Privately prompt for a native password").add_argument("username")
    for name in ("add-member", "remove-member"):
        membership = commands.add_parser(name, help="Change one explicit group membership")
        membership.add_argument("group")
        membership.add_argument("username")
    commands.add_parser("access", help="Show root ACLs, inheritance, rosters, and their limits").add_argument(
        "collection"
    )
    args = parser.parse_args(argv)

    from irods.exception import UserDoesNotExist
    from irods.session import iRODSSession

    with iRODSSession(irods_env_file=str(Path(args.environment_file).expanduser())) as session:
        if args.command == "setup":
            result = setup_store(
                session,
                args.collection,
                args.reader_group,
                args.writer_group,
                readers=args.readers,
                writers=args.writers,
                adopt_existing=args.adopt_existing,
            )
        elif args.command == "access":
            result = access_report(session, args.collection)
        elif args.command == "create-user":
            _require_admin(session)
            try:
                _regular_user(session, args.username)
            except UserDoesNotExist:
                changed = create_user(session, args.username, _read_password())
            else:
                changed = False
            result = {"user": args.username, "created": changed}
        elif args.command == "set-password":
            _require_admin(session)
            _regular_user(session, args.username)
            set_password(session, args.username, _read_password())
            result = {"user": args.username, "password_changed": True}
        else:
            changed = set_membership(session, args.group, args.username, add=args.command == "add-member")
            result = {"group": args.group, "user": args.username, "membership_changed": changed}
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
