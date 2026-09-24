============================
iRODS object store prototype
============================

The experimental ``IRODSObjectStore`` backend stores OpenGHG metadata in an
iRODS catalogue and versioned Zarr data as iRODS data objects. Configure its
factory to use the normal standardisation, search, retrieval, and data-management
entry points. Requested Zarr chunks are read through iRODS and verified in
memory. Persistent client data caching is disabled by default; a laptop can
read the remotely catalogued data without maintaining a local data cache.

This guide is for developers evaluating the backend with a dedicated test
collection. It describes the implemented interface and its limits; it does not
establish production readiness or compatibility with every scientific data type.
See :doc:`objectstore_backends` for the generic factory contract.
Operators creating a shared collection and assigning named readers and writers
should follow :doc:`irods_administration`.

Prepare a service and configure a store
=======================================

You need an accessible iRODS service, an authenticated account, an existing
collection dedicated to OpenGHG, and permission to read and write its data and
metadata. The collection and every ancestor except ``/`` must be visible to
catalogue queries and be ordinary collections. Linked and mounted collections
are rejected because their creation semantics cannot provide the writer lock
used here. Store paths must be absolute, with no traversal components, single
quotes, backslashes, or control characters.

The backend uses the authentication and TLS settings supplied by the Python
iRODS Client. Follow its `secure connection instructions
<https://github.com/irods/python-irodsclient#establishing-a-secure-connection>`__
and verify access to your service before running OpenGHG. The backend does not
configure a server or issue credentials.

From this checkout, install the optional client in your development environment:

.. code-block:: bash

   python -m pip install -e '.[irods]'

This installs ``python-irodsclient>=3.3,<4``, not an iRODS server or catalogue
database. See :doc:`quickstart_devel` for general environment setup.

Add an entry to the ``~/.openghg/openghg.conf`` created during OpenGHG setup.
Keep its existing ``user_id`` and ``config_version`` entries. Replace the
illustrative collection with one you have already created:

.. code-block:: toml

   [object_store.irods]
   path = "/yourZone/home/yourUser/openghg"
   permissions = "rw"
   factory = "openghg.objectstore._irods:irods_object_store"

   [object_store.irods.options]
   environment_file = "~/.irods/irods_environment.json"
   # resource = "an_existing_storage_resource"

``path`` is an iRODS logical collection, not a local directory. The constructor
requires it to exist and does not create a collection when opening a store.
``resource`` optionally chooses an existing resource for uploads; otherwise
iRODS applies its normal resource selection. If ``environment_file`` is omitted,
the factory uses ``IRODS_ENVIRONMENT_FILE`` or
``~/.irods/irods_environment.json``. Environment paths expand ``~``.
Omitting ``cache_dir`` disables persistent data caching; direct Python callers
can also pass ``cache_dir=None``. Use ``permissions = "r"`` for a laptop that
should only read.

Native iRODS authentication files may supply credentials. If you instead need
to pass a password from the process environment, add the following optional
mapping and arrange for that environment variable to be populated:

.. code-block:: toml

   [object_store.irods.credentials_env]
   password = "OPENGHG_IRODS_PASSWORD"

This records the environment-variable name, not its value. An unset mapped
variable raises a configuration error. Credentials remain in memory and are
excluded from datasource state and cache receipts; do not put passwords in
committed configuration or examples.

Use normal OpenGHG workflows
============================

The configured name, ``irods`` above, is passed through ``store=``. For example,
with a CRDS input file containing Bilsdale measurements from the 248 m inlet:

.. code-block:: python

   from openghg.standardise import standardise_surface
   from openghg.retrieve import get_obs_surface, search_surface

   standardise_surface(
       filepath="/path/to/bsd.picarro.1minute.248m.min.dat",
       source_format="CRDS",
       site="bsd",
       network="decc",
       store="irods",
   )
   results = search_surface(store="irods", site="bsd", species="co2", inlet="248m")
   print(results.metadata)
   observations = get_obs_surface(
       store="irods", site="bsd", species="co2", inlet="248m", version="latest"
   )
   assert observations is not None
   print(observations.data.isel(time=slice(0, 10)).load())

Replace the input path and the scientific identifiers together for your data.
The parser and normal data-type validation still run. Standardisation retains
its existing ``if_exists`` and ``save_current`` choices; retrieval accepts
``version="v1"`` and later versions. Metadata, dataset-attribute edits, custom
metadata-key configuration, and deletion use the normal data-management
interfaces. These reuse the shared OpenGHG implementations rather than a second
set of scientific update rules.

Search reads catalogue metadata without transferring Zarr payloads. The current
metastore scans the direct UUID collections for the requested data type and
applies the existing predicates to those records. It has no persistent TinyDB
file and no AVU search index. Catalogue round trips can therefore become
expensive as the number of datasources grows.

Try a small synthetic dataset directly
======================================

This example creates a fresh demonstration collection and leaves two versions
there for inspection. It uses constructed xarray data without invoking a parser
or checking a scientific data-type specification.

.. code-block:: python

   import os
   from uuid import uuid4

   import numpy as np
   import xarray as xr
   from irods.session import iRODSSession
   from openghg.objectstore import IRODSObjectStore

   original = xr.Dataset(
       {"ch4": ("time", [1900.0, 1901.0], {"units": "nmol mol-1"})},
       coords={
           "time": np.array(
               ["2025-01-01T00:00", "2025-01-01T01:00"], dtype="datetime64[ns]"
           )
       },
   )
   later = xr.Dataset(
       {"ch4": ("time", [1902.0], {"units": "nmol mol-1"})},
       coords={"time": np.array(["2025-01-01T02:00"], dtype="datetime64[ns]")},
   )
   environment = os.path.expanduser("~/.irods/irods_environment.json")
   with iRODSSession(irods_env_file=environment) as session:
       collection = f"/{session.zone}/home/{session.username}/openghg-demo-{uuid4().hex}"
       session.collections.create(collection, recurse=True)
       with IRODSObjectStore(
           session,
           collection,
           mode="rw",
           data_type="surface",
       ) as store:
           uuid = store.create(
               metadata={"species": "ch4", "site": "synthetic", "data_type": "surface"},
               data=original,
               period="3600s",
           )
           store.update(uuid, data=later, if_exists="auto", new_version=True)
           datasource = store.get_datasource(uuid)
           xr.testing.assert_equal(datasource.get_data(version="v1").load(), original)
           assert datasource.get_data(version="v2").sizes["time"] == 3
           assert store.search({"species": "ch4"})[0]["uuid"] == uuid
           print("Collection:", collection)
           print("UUID:", uuid)

A writable store must be entered with ``with`` before any mutations. The
constructor defaults to ``mode="r"``. The example borrows its supplied session:
keep that session open while using its datasources. Closing the store releases
its writer lock but does not close a borrowed session.

Use remote data and optional caching
====================================

Configure the laptop for the same service and logical collection. Its account
must have access to the collection and its ancestors. No cache directory is
needed. The configured factory opens sessions when needed, so a lazy dataset
remains usable after the ObjectStore context closes:

.. code-block:: python

   from openghg.objectstore import get_bucket, open_object_store

   with open_object_store(get_bucket("irods"), "surface", mode="r") as store:
       record = store.search({"site": "bsd", "species": "co2"})[0]
       datasource = store.get_datasource(record["uuid"])
       lazy_data = datasource.get_data(version="latest")

   selected = lazy_data.isel(time=slice(0, 10)).load()
   print(selected)

Opening an xarray dataset reads Zarr metadata and coordinate data. Loading a
selection fetches the data chunks needed for that selection; a chunk can be
larger than the selected range. With caching disabled, each storage-key read
transfers its bytes through iRODS into memory and verifies their checksum and
size. The backend writes no persistent downloaded data or receipts. Arrays
already loaded by xarray remain available in memory.

To retain verified downloads for reuse, explicitly add ``cache_dir`` to the
store's existing options table. Choose a private, writable work directory on
the client; replace the example ``<user>`` path for your system:

.. code-block:: toml

   [object_store.irods.options]
   environment_file = "~/.irods/irods_environment.json"
   cache_dir = "/work/<user>/openghg-irods-cache"

Cache paths expand ``~``, but there is no default home-directory cache.
Reopen the configured store after changing this option. Direct Python callers
can supply the same directory as ``IRODSObjectStore(..., cache_dir=...)``.
With caching enabled, the datasource's mapping also exposes cached files and
download receipts:

.. code-block:: python

   with open_object_store(get_bucket("irods"), "surface", mode="r") as store:
       datasource = store.get_datasource(record["uuid"])
       mapping = datasource.mapping(version="latest")
       # Pick a data chunk rather than a Zarr metadata object.
       key = next(k for k in mapping if not any(p.startswith(".") for p in k.split("/")))
       print(mapping.local_path(key))
       print(mapping.provenance(key))

``mapping.local_path(key)`` fetches or verifies the local file for an individual
Zarr key. ``mapping.provenance(key)`` returns its JSON receipt. Both methods
raise ``ValueError`` when persistent caching is disabled. Receipts identify the
remote endpoint, zone, logical path, catalogue data ID, checksum, size, replica,
resource, and download time. They do not establish scientific processing
history: record source identifiers and processing details in datasource metadata.

Both read modes check the live catalogue and require a supported checksum
(SHA-256 or legacy MD5) and consistent good replicas. Cached bytes are hashed
before reuse. Connection, permission, and integrity errors do not fall back to
local bytes. Cache hits avoid repeat payload transfers but still require
catalogue and local disk reads. The optional cache has no automatic eviction;
manage its disk usage explicitly. Coordinate access and many small chunks can
generate substantial request overhead with either mode.

An iRODS **replica** is a physical copy on a registered storage resource with
the same catalogue data ID as its sibling replicas. A normal client download
does not register the laptop filesystem as such a resource. See the
`iRODS data object and replica model
<https://github.com/irods/irods_docs/blob/main/docs/system_overview/data_objects.md>`__.
An administrator must provision another registered resource before requesting
managed replication:

.. code-block:: python

   from openghg.objectstore import get_bucket, locking_object_store

   with locking_object_store(get_bucket("irods"), "surface", mode="rw") as store:
       store.replicate(record["uuid"], resource="another_registered_resource")

Replication covers every Zarr object in every version of that datasource. Each
object retains its own data ID, and the backend checks destination replica
status, size, and checksum. Replicas describe placement of one object, while
OpenGHG versions describe dataset revisions. Making a laptop a managed iRODS
resource would require a reachable server and an operational design for its
intermittent connectivity. The implemented laptop workflow reads from the
remote store, with optional verified caching and no offline discovery or
offline fallback.

Persistence, writer locks, and failure recovery
===============================================

The existing ``ObjectStore`` facade coordinates the iRODS metastore and a
subclass of the normal ``Datasource``. ``VersionedZarrStore`` retains the shared
append, overlap, combination, and version behaviour. Each Zarr key is an iRODS
object beneath ``<root>/<data_type>/<uuid>/<version>/``; iRODS controls its
physical placement. Root-level standardisation and configuration documents,
raw records, and datasource state are JSON documents in collection metadata.
Numbered Attribute-Value-Units (AVU) chunks permit documents larger than one
AVU field. A manifest checks their completeness, and each document replacement
uses one atomic metadata operation.

The backend serializes writers across clients by creating
``<root>/.openghg-write-lock`` nonrecursively in the catalogue. A second writer
fails while that collection exists. Successful context exit removes the lock.
A crashed process or ambiguous connection failure can leave it behind; there
is no automatic expiry or ownership guessing. Before an operator removes an
abandoned lock, verify that no writer is still active and inspect the interrupted
operation's dataset state. Deleting a live writer's lock breaks serialization.

A catalogue metadata operation is atomic, but a whole dataset update is not.
Zarr chunks, version collections, datasource state, and raw records are separate
operations. Readers are not locked and do not receive snapshot isolation while
a writer changes data. Coordinate reads with writers when a consistent dataset
snapshot is required. Interrupted writes can leave partial or unpublished data
for inspection; the backend does not automatically roll back or reconcile them.
Do not edit the backend's AVUs directly.

Deletion moves payload objects and collections to iRODS trash. Unpublished
records disappear from OpenGHG search, while any explicitly enabled client cache
retains its downloaded files. The cache has no eviction, automatic deletion
propagation, local edit synchronisation, or independent version-retention policy.
It is not an offline object store or a backup. No automatic migration of existing
local OpenGHG stores is implemented.

Run the checks
==============

Unit tests exercise catalogue documents, verified in-memory reads, optional
cache integrity, read-only guards, writer-context guards, and shared Zarr
append, update, copy, and version deletion behaviour. To run them without a server:

.. code-block:: bash

   python -m pytest tests/objectstore/test_irods_storage.py tests/objectstore/test_irods_metastore.py tests/objectstore/test_irods_backend.py

The opt-in integration check requires an existing writable collection reserved
for tests. It creates temporary child collections and removes its test data:

.. code-block:: bash

   export OPENGHG_IRODS_TEST_COLLECTION='/yourZone/home/yourUser/openghg-test'
   export IRODS_ENVIRONMENT_FILE="$HOME/.irods/irods_environment.json"
   python -m pytest tests/objectstore/test_irods.py

The live fixture requires both variables, including an explicit environment
file. Set ``OPENGHG_IRODS_TEST_RESOURCE`` to an
existing second resource to exercise managed replication, including refresh
of an existing replica after an update. The account must be allowed to use it.
Without both required variables, the live checks are skipped.

A mock transport cannot establish server compatibility, authentication, network
routing, or resource policy. Record the actual client and server versions and
which workflows passed when reporting a live result. A successful surface-data
workflow does not establish coverage for every OpenGHG data type or parser.

The two live checks passed on iRODS 5.0.2 with Python iRODS Client 3.3.0 and
PostgreSQL 16.15. They exercised configured surface standardisation, historical
and current retrieval, metadata and attribute edits, custom metadata keys,
deletion, writer contention, version policies, large catalogue records, and
replication followed by refresh of a stale replica. Both storage resources were
on one test host. The relocated TLS service also passed the separate
:doc:`administration access check <irods_administration>` with native reader,
writer, and unprivileged accounts, including inherited permissions, server-side
write rejection, and reader revocation. A real laptop connection remains untested.

What else iRODS could provide
=============================

The catalogue and managed replicas provide a starting point for further iRODS
features. The following extensions need additional integration or server
configuration:

* **Native catalogue search.** Search data identifiers and
  AVUs with `GenQuery
  <https://github.com/irods/irods_docs/blob/main/docs/system_overview/genquery.md>`__
  while the server controls physical placement. Indexed AVUs could replace
  the prototype's metadata scan when its scale justifies the extra index
  maintenance.
* **Storage tiers and replica policies.** The `storage tiering plugin
  <https://github.com/irods/irods_capability_storage_tiering>`__ can migrate
  data between resources using age or metadata rules, preserve chosen
  replicas, verify transfers, and restage accessed data. This could keep
  active inversion inputs on fast storage while retaining archive copies.
* **Collaboration between institutions.** `Zone federation
  <https://github.com/irods/irods_docs/blob/main/docs/system_overview/federation.md>`__
  allows access across separately administered zones. Administrators must
  establish trust, networking, and remote users; federation is not automatic
  catalogue mirroring or conflict resolution.
* **Ingest from existing storage.** `Automated ingest
  <https://github.com/irods/irods_capability_automated_ingest>`__ can register
  existing files visible to a resource server, or ingest incoming files.
  Registration could support a later migration without copying every byte,
  once ownership and OpenGHG metadata mapping have been defined.
* **Enforced metadata and recorded operations.** `Metadata Guard
  <https://github.com/irods/irods_rule_engine_plugin_metadata_guard>`__ can
  restrict changes to selected AVU prefixes. The `AMQP audit plugin
  <https://github.com/irods/irods_rule_engine_plugin_audit_amqp>`__ can emit
  server policy events for an external audit service. Both require server
  configuration; neither automatically records scientific derivation.
* **Additional storage systems.** The `S3 resource plugin
  <https://github.com/irods/irods_resource_plugin_s3>`__ can place data in
  compatible object storage while retaining iRODS catalogue access.

Deployment and next experiments
===============================

An iRODS service needs a catalogue database, persistent resource storage,
authentication, network access, backups, and an operational owner. Official
`installation information <https://irods.org/download/>`__ describes server
and database plugin packages. A user-owned container can provide a test service
where the host permits it; a compiler or Pixi environment alone does not
provision the service. Blue Pebble hostnames, credentials, and run instructions
belong in private deployment notes on that system, outside Git.

The next useful evaluations are:

1. Connect a laptop through the site's supported network route and test its
   authentication, uncached reads, optional cache reuse, interrupted downloads,
   and permission changes.
2. Measure catalogue scan latency, per-chunk connection overhead, and update
   cost with representative datasets. Indexes, batching, or connection pooling
   may be justified by those measurements.
3. Design publication and recovery for partial multi-object writes, reader
   coordination, cache limits, and retention before moving production stores.
4. Plan migration and operational policies together with the service owner,
   including server backups and replica or tiering policies.

The generic factory also leaves room for a future fsspec-based backend. No
fsspec transport is implemented by this change; such a backend would still need
to provide metadata, document persistence, locking, and lazy-session semantics.
The iRODS adapter demonstrates the transport and catalogue integration, while
service operation and production consistency remain separate work.
