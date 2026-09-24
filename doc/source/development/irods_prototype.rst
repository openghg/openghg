============================
iRODS object store prototype
============================

``IRODSObjectStore`` is an experimental backend for evaluating iRODS as the
catalogue and storage service behind OpenGHG. It stores an ``xarray.Dataset``
snapshot in iRODS, searches its catalogue metadata, and downloads requested
data into a local cache with a provenance receipt.

This guide is for developers testing the backend directly. The normal
``standardise_*``, ``get_obs_*``, and configured local object store workflows
do not select this backend. Start with a dedicated collection and synthetic
data, as in the example below.

How the prototype fits together
===============================

The existing ``ObjectStore`` class already coordinates a ``MetaStore`` and a
datasource factory. ``IRODSObjectStore`` uses that interface with an iRODS
metastore and datasource implementation; a second abstract facade is not
needed for this experiment.

Each datasource has an OpenGHG UUID and one immutable NetCDF snapshot at
``<collection>/<uuid>.nc``. Metadata is stored in the iRODS catalogue as
Attribute-Value-Units (AVUs): a JSON record preserves metadata values, and
per-field AVUs provide catalogue search indexes. The application still chooses
logical object names, while iRODS manages the mapping to physical storage.
Metadata must be JSON-compatible. The complete encoded record, including the
generated UUID, is limited to 2700 UTF-8 bytes in this prototype. This is a
conservative bound for the catalogue AVU field; large processing manifests
need a different representation before they can be stored here.

``search()`` and ``retrieve()`` inspect catalogue metadata without downloading
the NetCDF data. A returned datasource downloads its snapshot when
``get_data()`` or ``local_path()`` is called. The cache verifies file size and
checksum before publishing the downloaded file. Its identity includes the
remote endpoint, zone, catalogue data ID, logical path, and checksum, so
unrelated stores and changed content do not share a cache entry.

The ``provenance`` property exposes the cache receipt after data retrieval.
Accessing that property also fetches and verifies the snapshot if necessary.
The receipt records where the bytes came from and their identity without storing
authentication credentials. It does not establish the scientific processing
history: record source identifiers, processing versions, and revision
relationships in datasource metadata when creating a snapshot.

Try a synthetic snapshot
========================

You need an accessible iRODS service, an account allowed to create data and
metadata in a dedicated collection, and a configured iRODS client environment.
The client uses the authentication and TLS settings supplied through that
environment; the prototype does not configure a server or issue credentials.
See the `Python iRODS Client connection instructions
<https://github.com/irods/python-irodsclient#establishing-a-secure-connection>`__.

From this OpenGHG checkout, install the optional client dependency in your
development environment:

.. code-block:: bash

   python -m pip install -e '.[irods]'

The extra uses ``python-irodsclient>=3.3,<4``. It is a Python client, so
installing this extra does not install an iRODS server or catalogue database.
See :doc:`quickstart_devel` for general development environment setup.

Run the following with your normal iRODS client environment. It creates a
new demonstration collection on every run and leaves the synthetic snapshot
there for a later laptop retrieval.

.. code-block:: python

   import os
   from pathlib import Path
   from uuid import uuid4

   import numpy as np
   import xarray as xr
   from irods.session import iRODSSession
   from openghg.objectstore import IRODSObjectStore

   dataset = xr.Dataset(
       {"ch4": ("time", [1900.0, 1901.0], {"units": "nmol mol-1"})},
       coords={"time": np.array(["2025-01-01", "2025-01-02"], dtype="datetime64[ns]")},
   )

   environment = os.path.expanduser("~/.irods/irods_environment.json")
   with iRODSSession(irods_env_file=environment) as session:
       collection = f"/{session.zone}/home/{session.username}/openghg-demo-{uuid4().hex}"
       session.collections.create(collection, recurse=True)
       store = IRODSObjectStore(
           session,
           collection,
           cache_dir=Path.home() / ".cache" / "openghg-irods-demo",
           mode="rw",
       )
       uuid = store.create(
           metadata={"species": "ch4", "site": "synthetic", "revision": "1"},
           data=dataset,
       )
       print("Collection:", collection)
       print("UUID:", uuid)

       records = store.search(species="ch4")
       assert records[0]["uuid"] == uuid
       datasource = store.get_datasource(uuid)
       recovered = datasource.get_data()
       xr.testing.assert_equal(recovered, dataset)
       print(datasource.local_path())
       print(datasource.provenance)

This uses already constructed xarray data. It does not run an OpenGHG parser
or validate a scientific data-type specification.

Keep the ``iRODSSession`` open while using the store or its datasources. The
store borrows that session and does not close it on your behalf. The
constructor requires an existing collection. Its default mode is ``"r"``;
use ``"rw"`` to create snapshots or modify catalogue metadata. The optional
``resource`` argument selects an existing iRODS resource for uploads; leaving
it unset allows normal iRODS resource selection.

Use the same data from a laptop
===============================

Configure the laptop's iRODS client environment for the same remote service
and account, or another account granted access to the collection. With
``collection`` and ``uuid`` set to the values printed above:

.. code-block:: python

   import os
   from pathlib import Path

   from irods.session import iRODSSession
   from openghg.objectstore import IRODSObjectStore

   with iRODSSession(
       irods_env_file=os.path.expanduser("~/.irods/irods_environment.json")
   ) as session:
       store = IRODSObjectStore(
           session, collection, cache_dir=Path.home() / ".cache" / "openghg-irods"
       )
       datasource = store.get_datasource(uuid)  # Catalogue access only.
       data = datasource.get_data()  # Downloads and verifies on a cache miss.
       print(datasource.provenance)

This is a local cache of remotely catalogued data. In iRODS terminology a
**replica** is a physical copy on a registered storage resource, associated
with the same catalogue data ID as its sibling replicas. A normal client
download does not register the laptop filesystem as a storage resource. See
the `iRODS data object and replica model
<https://github.com/irods/irods_docs/blob/main/docs/system_overview/data_objects.md>`__.

For a managed replica, an administrator first provisions another resource
within the iRODS zone. A writable store can then request replication:

.. code-block:: python

   store.replicate(uuid, resource="another_registered_resource")

This delegates the transfer to iRODS. The resource name is not a laptop
directory. Making a laptop a managed resource would require an iRODS server
and reachable storage there; intermittent connectivity and network routing
would need operational design. A remote iRODS service with the client cache
is the simpler starting point for an intermittently connected laptop, but
the current prototype still requires catalogue access and does not provide
offline discovery.

Every cache access checks the remote catalogue and hashes the local file,
even on a cache hit. A supported catalogue checksum (SHA-256 or legacy MD5)
and consistent good replicas are required. Connection, permission, and
integrity errors are reported without falling back to potentially stale local
bytes. Repeated access saves network transfer but still incurs catalogue and
local disk reads.

Metadata, revisions, and prototype limits
=========================================

``store.update(uuid, metadata={...})`` updates catalogue metadata. Snapshot
bytes cannot be updated in place: create a new UUID with distinct revision
metadata instead. For example, record ``revision="2"`` and
``derived_from=<previous UUID>`` in the new snapshot's metadata. These are
application conventions, not an automatically maintained revision history.
iRODS replicas describe placement and consistency of one data object;
they do not supply OpenGHG's scientific versioning policy.

Use one writer for this prototype. A metadata operation can be atomic within
the iRODS catalogue, but an upload and its metadata publication are separate
operations. The prototype does not provide distributed transactions or
concurrent writer conflict resolution. Do not edit its catalogue AVUs directly:
the JSON record and field indexes must agree.

If upload completes but publication fails, the new object is removed where
possible. An interrupted upload can leave an unpublished object for an
administrator to inspect; the client cannot safely infer ownership after an
ambiguous transfer failure. Unpublished objects do not appear in searches.
Provenance receipts retain metadata captured at download time; later catalogue
metadata edits are visible through the datasource's ``metadata`` attribute.

The first implementation deliberately uses whole NetCDF snapshots. A small
time selection still requires downloading the snapshot, and ``get_data()``
loads it fully into memory. For application-managed lazy xarray reads, open
the path returned by ``local_path()`` instead. There is no remote
Zarr chunk access, append operation, or automatic migration of existing
OpenGHG stores. There is also no automatic synchronisation of local changes,
remote deletion propagation, cache eviction, or version retention policy.
Treat the cache as a working copy with a receipt, not an independent backup
or an offline object store.

``store.delete(uuid)`` moves the remote object and its AVUs to iRODS trash.
Existing local cache files are retained. Later access through the store still
requires a published remote object, so those retained files do not provide an
offline fallback.

Run the live integration check
==============================

The tests include an opt-in check against a real service. Supply an existing,
writable collection reserved for testing and your iRODS client environment:

.. code-block:: bash

   export OPENGHG_IRODS_TEST_COLLECTION='/yourZone/home/yourUser/openghg-test'
   python -m pytest tests/objectstore/test_irods.py::test_live_irods_round_trip

The collection is illustrative; replace it with one you control. Without
``OPENGHG_IRODS_TEST_COLLECTION``, the live check is skipped. Unit checks use
a test double and do not establish server compatibility, authentication,
network behaviour, or storage resource policy. Record the actual server and
client versions when reporting a live result.

Set ``IRODS_ENVIRONMENT_FILE`` if the test should use a client environment
file other than ``~/.irods/irods_environment.json``. Optionally set
``OPENGHG_IRODS_TEST_RESOURCE`` to the name of a second registered resource
to include managed replication in the live check. This resource must already
exist and permit the test account's replication operation.

What else iRODS could provide
=============================

These are iRODS capabilities to evaluate, rather than features enabled by
the OpenGHG prototype:

* **A shared catalogue and logical namespace.** Search data identifiers and
  AVUs with `GenQuery
  <https://github.com/irods/irods_docs/blob/main/docs/system_overview/genquery.md>`__
  while the server controls physical placement. This could remove the need
  for clients to understand the server's file layout.
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

The client adapter is only part of the work. An iRODS service needs a
catalogue database, persistent resource storage, authentication, network
access, and an owner responsible for operation and backup. Official
`installation information <https://irods.org/download/>`__ describes server
and database plugin packages. A user-owned container can be a useful test
deployment where the host permits it; a compiler or Pixi environment alone
does not provision the service. Site-specific hostnames, credentials, and
Blue Pebble run instructions belong in local deployment notes, outside Git.

The next useful milestones are:

1. Run the synthetic and live checks against the intended server, then test
   a laptop connecting through the site's supported network route.
2. Exercise two registered resources and inspect the common data ID,
   replica checksums, interrupted transfers, and stale replicas.
3. Measure metadata query latency and whole-file transfer costs on
   representative datasets before choosing NetCDF snapshots or remote
   chunked storage.
4. Design publication, revision retention, cache limits, and concurrent
   writer behaviour before integrating the standardisation and retrieval
   entry points or migrating real stores.

The operational service and the semantics of versioning and synchronisation
need separate evaluation. This prototype establishes an interface to test
those choices; it does not estimate production readiness from a successful
client round trip.
