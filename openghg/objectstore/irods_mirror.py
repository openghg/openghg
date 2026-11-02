"""Populate and inspect registered iRODS resource mirrors of published datasets.

Run ``python -m openghg.objectstore.irods_mirror --help`` for the CLI. A resource
mirror shares the authoritative catalogue; it is not an offline catalogue copy.
These commands never trim source replicas or delete retained generations.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import json
import os
from pathlib import Path
from typing import Any

from openghg.objectstore._irods import IRODSObjectStore
from openghg.objectstore._irods_storage import IRODSZarrMapping, _on_resource, _snapshot
from openghg.types import ObjectStoreError


def replica_report(
    store: IRODSObjectStore,
    resource: str,
    uuids: Sequence[str] | None = None,
    *,
    verify: bool = False,
) -> dict[str, Any]:
    """Report replica coverage of the observed publications on one resource.

    All logical versions of each selected datasource are included. By default,
    this inspects catalogue status, sizes and registered checksums only. Set
    ``verify=True`` to read and verify every available target object in memory.
    Connection, permission and integrity errors propagate; there is no fallback
    to another resource. The result records each observed publication revision,
    rather than claiming one atomic snapshot across the entire store.
    """
    if not isinstance(resource, str) or not resource.strip():
        raise ValueError("A registered destination resource is required.")
    selected = store.uuids if uuids is None else list(dict.fromkeys(uuids))
    records: list[dict[str, Any]] = []
    with store._sessions() as session:
        session.resources.get(resource)
        for uuid in selected:
            datasource = store.get_datasource(uuid)
            record: dict[str, Any] = {
                "uuid": uuid,
                "revision": datasource.revision,
                "versions": list(datasource._store.versions),
                "objects": 0,
                "bytes": 0,
                "good_objects": 0,
                "missing_objects": 0,
                "unavailable_objects": 0,
                "payloads_verified": 0,
            }
            for version in record["versions"]:
                source = datasource.mapping(version)
                target = IRODSZarrMapping(
                    store._sessions, source.collection, read_only=True, read_resource=resource
                )
                keys = list(source)
                if not keys:
                    raise ObjectStoreError(
                        f"No catalogued Zarr objects in published generation {source.collection}."
                    )
                for key in keys:
                    path = source.collection + "/" + key
                    identity = _snapshot(session, path)
                    replicas = [
                        replica
                        for replica in session.data_objects.get(path).replicas
                        if _on_resource(replica, resource)
                    ]
                    record["objects"] += 1
                    record["bytes"] += identity["size"]
                    good = any(
                        str(replica.status) == "1"
                        and replica.checksum == identity["checksum"]
                        and int(replica.size) == identity["size"]
                        for replica in replicas
                    )
                    if good:
                        record["good_objects"] += 1
                        if verify:
                            target[key]
                            record["payloads_verified"] += 1
                    elif replicas:
                        record["unavailable_objects"] += 1
                    else:
                        record["missing_objects"] += 1
            records.append(record)
    return {
        "collection": store.collection,
        "data_type": store.data_type,
        "resource": resource,
        "complete": all(record["good_objects"] == record["objects"] for record in records),
        "datasources": records,
    }


def sync_replicas(
    store: IRODSObjectStore,
    resource: str,
    uuids: Sequence[str] | None = None,
    *,
    verify: bool = False,
) -> dict[str, Any]:
    """Fill all published versions of selected datasources, then report coverage.

    Each datasource's publication is pinned while copying; catalogue writers
    are not locked out. Existing good replicas are reused and interrupted runs
    can be rerun. The final report observes current publications, so a concurrent
    update can leave coverage incomplete. No old replicas are deleted. Native
    replica creation requires modification permission even on a read-only handle.
    """
    selected = store.uuids if uuids is None else list(dict.fromkeys(uuids))
    for uuid in selected:
        store.replicate(uuid, resource)
    return replica_report(store, resource, None if uuids is None else selected, verify=verify)


def main(argv: Sequence[str] | None = None) -> int:
    """Run explicit mirror synchronisation or a read-only coverage check.

    Return zero for complete coverage, or two when objects are missing or stale.
    Credentials and TLS settings come from the supplied native environment.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--environment-file",
        default=os.environ.get("IRODS_ENVIRONMENT_FILE", "~/.irods/irods_environment.json"),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("status", "sync"):
        command = commands.add_parser(name)
        command.add_argument("collection", help="Existing logical ObjectStore collection")
        command.add_argument("--resource", required=True, help="Registered destination resource")
        command.add_argument("--data-type", required=True, help="OpenGHG data type to mirror")
        command.add_argument("--uuid", action="append", dest="uuids", help="Select UUID; repeat as needed")
        command.add_argument(
            "--verify", action="store_true", help="Read and checksum every target object without a disk cache"
        )
    args = parser.parse_args(argv)

    from irods.session import iRODSSession

    with iRODSSession(irods_env_file=str(Path(args.environment_file).expanduser())) as session:
        with IRODSObjectStore(
            session,
            args.collection,
            mode="r",
            data_type=args.data_type,
        ) as store:
            operation = sync_replicas if args.command == "sync" else replica_report
            result = operation(store, args.resource, args.uuids, verify=args.verify)
    print(json.dumps(result, indent=2))
    return 0 if result["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
