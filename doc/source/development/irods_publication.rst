=================================================
iRODS publication, snapshots, and write conflicts
=================================================

The iRODS backend publishes each datasource through one atomic catalogue
manifest. A reader pins the raw search metadata, datasource state, and payload
generation paths from that manifest. Updating a datasource prepares new Zarr
objects before publishing their references, so readers can keep using their
original dataset while the update runs. These guarantees apply to clients using
this backend and its writer context; they do not constrain direct administrator
edits to iRODS objects or catalogue metadata.

See :doc:`irods_prototype` for service prerequisites, configuration, and the
normal standardisation and retrieval interfaces. This page explains the
publication format, conflict handling, and retention boundaries.

Read a stable dataset
======================

A loaded datasource is a snapshot. Its ``latest`` version means the latest
version in that snapshot, even after another writer publishes a newer version.
Load the datasource again to see the current publication:

.. code-block:: python

   from openghg.objectstore import get_bucket, open_object_store

   bucket = get_bucket("irods")
   with open_object_store(bucket, "surface", mode="r") as store:
       record = store.search({"site": "bsd", "species": "co2"})[0]
       uuid = record["uuid"]
       source = store.get_datasource(uuid)
       old_revision = source.revision
       lazy = source.get_data()

   # Other clients may update or logically delete this datasource here.
   original_data = lazy.load()

   with open_object_store(bucket, "surface", mode="r") as store:
       current = store.get_datasource(uuid)  # Raises if it was deleted.
       print(current.revision, old_revision)

Choose metadata matching a datasource in your collection for the search above.
The factory must be able to reconnect when lazy data are loaded. Live catalogue
access, read permissions, and retained generations are still required. A
previously downloaded cache entry does not provide an offline fallback.
``datasource.mapping(version)`` exposes a read-only transport pinned to that
version's generation.

Logical versions such as ``v1`` and ``v2`` retain the normal OpenGHG overlap and
update rules. Updating ``v1`` with ``new_version=False`` now changes its
**generation**, rather than overwriting its existing objects. Attribute edits
also create a new generation. This lets the old snapshot and the newly loaded
snapshot each have a consistent ``v1`` with different contents.

Detect a stale writer
======================

Each publication has an opaque revision token. Loaded writable datasources
check that token before mutation and save. If another operation published in the
meantime, ``PublicationConflictError`` requires the caller to reload and
reconsider its changes. The backend does not automatically merge stale edits.
For an update based on an earlier read, supply that read's revision explicitly:

.. code-block:: python

   from openghg.objectstore import PublicationConflictError, open_object_store

   with open_object_store(bucket, "surface", mode="rw") as store:
       try:
           store.update(
               uuid,
               metadata={"comment": "reviewed"},
               expected_revision=old_revision,
           )
       except PublicationConflictError:
           current = store.get_datasource(uuid)
           # Compare the current state and decide whether to reapply the edit.

Without ``expected_revision``, ``update`` deliberately starts from the current
publication inside the writer context. A datasource handle still protects its
own loaded revision automatically. Store-level documents also track revisions
read through that ObjectStore handle; this catches a stale ``BaseStore.save``.
A fresh handle that writes a document without reading it first expresses an
unconditional replacement of its current value.

The root's native writer-lock collection serializes cooperating writers across
clients. Revision checks run while that lock is held. Atomic AVU replacement
alone is not a server-side conditional write, so bypassing the lock invalidates
the conflict guarantee. One ObjectStore handle permits nested contexts in its
own thread and rejects mutation or context use from another thread. Internal
parallel chunk transfers remain supported.

A failed mutation or failed publication makes that datasource handle unusable
for further writes; reload before retrying. This includes an ambiguous failure
where the server committed but its acknowledgement was lost. Staged transports
are frozen before publication, so such a failure cannot leave a writable handle
to a possibly published generation. An abandoned root lock still needs deliberate
operator recovery after verifying that no writer remains active; there is no
automatic expiry or lock breaking.

Publication format and failure boundaries
==========================================

Each UUID collection contains a chunked JSON ``publication`` document with:

* its format version and publication revision;
* the raw searchable ``record`` and serialized ``datasource`` state; and
* a mapping from each logical version to its immutable generation collection.

Generations live under
``<root>/<data_type>/<uuid>/generations/<generation_uuid>/``. The revision and
transport paths are backend state, excluded from common datasource attributes.
Publishing replaces all AVUs of this one document in a single atomic operation.
Search only reads catalogue documents, and final merged search metadata comes
from the same publication as the loaded datasource.

A failed upload can leave an unreferenced generation; a failed catalogue commit
leaves the old publication visible. No reader discovers a staged generation by
scanning its collection. Old generations remain unchanged, including their data
IDs and replicas. A replacement generation contains new data objects with new
data IDs; replication preserves identity among copies of each of those objects.

An update to an existing logical version currently copies all of its Zarr keys
through the client before applying changes. Metadata-only edits do not copy
payloads. This is intentionally a straightforward implementation, not chunk-level
copy-on-write or a server-side cloning optimization. Measure update cost before
using large datasets.

Atomicity is per datasource publication. A search across multiple datasources,
a multi-file standardisation call, and updates to datasource publications plus
store-level documents are not one transaction. A concurrent search can observe
different committed revisions for different datasources. Explicit stale-document
errors prevent silent lost updates but do not roll back datasource operations
that already committed earlier in a workflow.

Deletion, retention, and legacy stores
======================================

Deleting a version removes its reference from the next publication. Deleting a
datasource publishes a tombstone, making it disappear from new searches and
loads. Both retain physical generations, so readers already holding snapshots
can finish. Deletion therefore does **not** reclaim remote storage or move
payloads to trash. There is currently no automatic garbage collection, expiry,
reader lease, or version restoration API. Future reclamation must define a
retention window or coordinate readers; deleting a retained generation manually
can break those readers even if its bytes were cached locally.

Existing prototype stores with separate ``record`` and ``datasource`` documents
remain readable when both documents exist and validate. They receive a
content-derived revision token. The first locked write creates the combined
manifest. It preserves the old payload collections and creates a new generation
when data are changed. Incomplete legacy records stay unpublished; malformed
published metadata raises an error. Migration does not repair missing or corrupt
legacy payloads.

Upgrade every writer before using an existing collection with this format.
Older clients ignore the new manifest and may overwrite legacy paths or publish
obsolete metadata. Previously retained legacy snapshots are safe only once all
writers follow the new protocol. New code treats the combined manifest,
including a tombstone, as authoritative even while legacy documents remain.
Local filesystem ObjectStores keep their existing persistence model; this change
does not migrate them or add these snapshot guarantees to them.

Verify the guarantees
======================

The focused regression suite covers readers during publication, interrupted
uploads, lost acknowledgements, stale datasource and document writes, retained
readers after deletion, generation immutability, and legacy migration:

.. code-block:: bash

   python -m pytest tests/objectstore/test_irods_publication.py

Run the server integration checks described in :doc:`irods_prototype` as well.
Mock transport tests establish backend behaviour, not deployment networking,
authentication, server compatibility, or protection from administrative edits.
