=========================================
Mirror an iRODS store onto another server
=========================================

An OpenGHG iRODS store can use a second server as a shared mirror for many
readers. An authorised operator populates its registered storage resource;
readers then use the same replicas through their own iRODS accounts. A trusted
account can also create missing replicas when data are requested. Each
replicated Zarr object keeps its logical path and catalogue data ID; the
catalogue records its replica number, resource, status, size, and checksum.

This guide covers an **online resource mirror** in one iRODS zone. The original
catalogue remains authoritative and must be reachable. It does not create a
second catalogue, an independent OpenGHG store, or an offline copy of search
metadata. See :doc:`irods_prototype` for the basic backend and
:doc:`irods_publication` for immutable generations and reader snapshots.

Prepare the two servers
=======================

The source server is the catalogue **provider**. A second server, the
**consumer**, joins the same zone and supplies a registered storage resource,
called ``mirrorResc`` in this guide. The consumer needs no separate catalogue
database. An administrator must provision the consumer, its private vault, and
the resource before enabling mirroring in OpenGHG.

A consumer is a trusted zone server, not an ordinary authenticated client.
It uses the zone's server-to-server authentication secret, negotiation key,
zone identity, and service-account credentials. These are more privileged than
a reader's personal credentials. Do not distribute zone configuration or
administrator authentication files as part of normal reader onboarding.
The zone operator must accept the mirror server into the service's trust
boundary. Readers do not each need a consumer server or a separate resource.
Keep server secrets and the vault private; never edit replica files directly.

Both servers need stable names that resolve to reachable endpoints, the same
zone communication port, and verified TLS connections in both directions.
Configure ``tls_server`` certificates and ``tls_client`` trust on each server,
require encrypted connections, and retain hostname verification. Certificates
must cover the names used by clients and peer servers. iRODS 5.0.2's native
hostname checker compares DNS subject-alternative names; an IP-address SAN
alone does not satisfy that checker. Prefer stable DNS names with matching
certificates. See the official `installation guide
<https://github.com/irods/irods_docs/blob/main/docs/getting_started/installation.md>`__,
`server configuration reference
<https://github.com/irods/irods_docs/blob/main/docs/system_overview/configuration.md>`__,
and `iRODS 5.0.2 hostname-check implementation
<https://github.com/irods/irods/blob/5.0.2/lib/core/src/sslSockComm.cpp#L755-L827>`__.

A routed private network or VPN is the simplest deployment model. The mirror
server must reach the provider for catalogue operations, and the servers must
reach each other for resource operations. Readers need authenticated access to
the mirror endpoint. A client-only SSH local forward does not create
this complete topology. Reverse forwards, hostname translation, container
network namespaces, and the common zone port need an explicit design and an
end-to-end test. Opening one forwarded client port is not evidence that native
replication will work. The backend does not provision VPNs, tunnels, DNS, or
server trust.

The OpenGHG replication helper requests native transfers without parallel
portal connections. It uses a separate client session with ``numThreads=-1``;
ordinary payload downloads use one client stream. Other iRODS tools or site
policies may use the configured parallel-transfer port range. Keep the relevant
network policy explicit instead of assuming every iRODS operation uses only the
control port. The distinction between automatic thread selection and disabled
threading is defined in `iRODS 5.0.2 transfer selection
<https://github.com/irods/irods/blob/5.0.2/server/core/src/objDesc.cpp#L354-L382>`__.

Choose who may create replicas
==============================

Native iRODS 5.0.2 replication requires at least ``modify_object`` permission
on each logical object. ``read_object`` permits reading an existing replica
but does not permit creating or refreshing one. This is enforced by the
`native replication permission check
<https://github.com/irods/irods/blob/5.0.2/server/api/src/rsDataObjRepl.cpp#L484-L532>`__.
There is no separate replication-only permission in this interface.

Choose one of these workflows:

* Keep ordinary reader credentials and have an operator populate ``mirrorResc``
  using ``sync`` below. Configure strict reads from that resource with
  ``replicate_on_read = false``. Missing replicas produce an error until the
  operator synchronises them.
* For a trusted account with modification permission, explicitly enable
  ``replicate_on_read = true``. The account can then create or refresh replicas
  as part of a read. This permission also allows native content modification;
  OpenGHG's local read-only setting does not narrow the server-side grant.

The shared-store reader role in :doc:`irods_administration` remains
``read_object``. The mirroring commands do not promote it, issue credentials,
or install a privileged replication service. A site that requires automatic
mirroring for ordinary readers needs a separately designed and tested server
policy or service with a restricted interface.

Configure the mirror on the client
==================================

Add a store entry pointing to the **same logical collection** as the source.
Use the mirror server's authenticated client endpoint in each reader's iRODS
environment file so that server supplies the selected replica bytes. The
endpoint still needs the provider for catalogue access. Keep the existing
top-level OpenGHG settings:

.. code-block:: toml

   [object_store.mirror]
   path = "/yourZone/projects/observations"
   permissions = "r"
   factory = "openghg.objectstore._irods:irods_object_store"

   [object_store.mirror.options]
   environment_file = "~/.irods/mirror_environment.json"
   read_resource = "mirrorResc"
   replicate_on_read = false

``read_resource`` selects a registered resource by name for payload reads.
The backend requires a good replica on the selected resource with the expected
checksum and size. It does not silently fetch another resource when that
replica is missing, stale, or inaccessible. The existing ``resource`` option
still controls uploads; setting it alone does not select a mirror for reads.

To populate missing replicas automatically, change ``replicate_on_read`` to
``true`` using an account with the permission described above. This option
requires ``read_resource``. The store can remain ``permissions = "r"`` because
OpenGHG datasource content and publication metadata are not being edited;
iRODS replica placement is still a catalogue mutation. Existing good target
replicas are reused, and newly created replicas are checked before their bytes
are returned. Connection, permission, and integrity failures propagate.

Neither option enables the separate persistent client byte cache. With no
``cache_dir``, downloaded bytes are verified in memory. If explicitly enabled,
that cache remains an additional client copy with provenance receipts; its files
are not registered iRODS replicas. Live catalogue checks still apply to cache
hits. See :doc:`irods_prototype` for cache configuration and limitations.

Use the normal retrieval API with the configured name:

.. code-block:: python

   from openghg.retrieve import get_obs_surface, search_surface

   results = search_surface(store="mirror", site="bsd", species="co2")
   print(results.metadata)
   observations = get_obs_surface(
       store="mirror", site="bsd", species="co2", inlet="248m"
   )
   assert observations is not None
   print(observations.data.isel(time=slice(0, 10)).load())

Choose identifiers that match your stored observations. Search reads catalogue
metadata and does not populate the mirror. Opening and loading a dataset reads
its Zarr metadata, coordinates, and requested data chunks; those object reads
trigger replication when enabled. This can copy fewer objects than a full
synchronisation. Lazy arrays retain their publication snapshot and may make
further requests when computed, including after the configured store context
closes. The consumer, provider, credentials, and retained generation must remain
available for those requests.

Populate and inspect a mirror explicitly
========================================

The module ``openghg.objectstore.irods_mirror`` provides ``status`` and ``sync``.
It takes a native iRODS environment file and a logical collection directly;
these commands do not read the named OpenGHG store configuration. Install the
``irods`` extra first. The environment file contains authentication and TLS
settings for the endpoint that can reach both resources.

Check the published surface datasets without downloading their payloads:

.. code-block:: bash

   python -m openghg.objectstore.irods_mirror \
       --environment-file "$HOME/.irods/mirror_environment.json" \
       status /yourZone/projects/observations \
       --resource mirrorResc --data-type surface

The JSON report records observed datasource revisions, versions, object counts,
byte counts, and good, missing, or unavailable target replicas. Exit status
``0`` means complete catalogue coverage; ``2`` means incomplete coverage.
A catalogue report does not prove that every physical file can be read.
Add ``--verify`` to download and checksum available good target objects in
memory, without a persistent disk cache:

.. code-block:: bash

   python -m openghg.objectstore.irods_mirror \
       --environment-file "$HOME/.irods/mirror_environment.json" \
       status /yourZone/projects/observations \
       --resource mirrorResc --data-type surface --verify

An operator can populate every published version of the selected data type:

.. code-block:: bash

   python -m openghg.objectstore.irods_mirror \
       --environment-file "$HOME/.irods/operator_environment.json" \
       sync /yourZone/projects/observations \
       --resource mirrorResc --data-type surface --verify

Append ``--uuid <datasource-uuid>`` to select one datasource; repeat the flag for
several. Without it, the command uses all currently visible datasources of the
specified data type. Use a UUID from search results, not a catalogue object ID.

``sync`` uses a read-only OpenGHG handle and does not acquire the root writer
lock. It still requires native ``modify_object`` permission to create or refresh
replicas; a read-only handle does not grant that permission. Large transfers can
therefore run while source clients continue publishing datasets. The command
reuses good replicas, fills missing replicas, and refreshes stale replicas. It
does not rewrite scientific data, create dataset versions, remove source
replicas, or delete older generations. Completed replicas remain if a later
transfer fails, so the operation can be retried after resolving the error.

Each datasource's publication is pinned independently while its immutable
objects are copied. Neither ``sync`` nor ``status`` establishes a global mirror
checkpoint or locks out writers. The initial UUID selection determines which
datasources are copied. Without ``--uuid``, the final report refreshes the UUID
list, so datasources created during the transfer can appear as missing or
incomplete even though they were not selected for copying. With ``--uuid``, the
final report stays within that explicit selection. Both reports observe current
publications: a concurrent update can publish a new generation that has not yet
been replicated, leaving coverage incomplete. Run ``sync`` again to fill new
generations and newly created datasources.

If a selected datasource is logically deleted during the operation, its final
lookup can fail even though some replicas were created successfully. The
command leaves those completed copies intact. Refresh search results, remove
any deleted UUID from an explicit selection, and retry; do not delete retained
generations to recover a partial sync. Source publication and logical deletion
continue to follow :doc:`irods_publication`.

Updates, outages, and retained data
===================================

A source update publishes a new immutable generation. Already opened readers
keep their original generation; newly opened readers see the new publication.
The mirror copies the new objects on demand or during the next ``sync`` run.
All readers share those replicas; another reader does not require another copy.
The catalogue can expose a new publication before its objects have reached the
mirror. A strict reader without replication permission will then fail to load
the missing objects until the operator synchronises them. Run ``sync`` after
ingestion or invoke it through the site's normal scheduled operator job. This
backend does not include a background scheduler or atomic publication barrier
that waits for every mirror.

The initial implementation requires the provider's catalogue to remain online,
even when all required payload replicas are on the mirror server. It provides no
offline discovery, fallback, or disconnected writes. Strict reads also require
the selected replica's resource to be usable. An unavailable source resource
need not prevent reading an existing good mirror replica when the catalogue
and consumer are reachable; this is different from taking the provider down.

Logical deletion and version removal do not reclaim retained generations or
their replicas. The mirror has no automatic eviction, storage quota manager,
or garbage collector. An operator must account for old snapshots before
trimming replicas or removing a resource. Revoking an account blocks later
catalogue-authorised reads; it cannot recall bytes already downloaded or remove
physical copies from a machine controlled by their recipient.

Test with a local two-server zone
=================================

The repository's ``tests/integration/irods_mirror`` directory provides a
self-contained local test zone suitable for trying the topology on a laptop.
It includes a provider, consumer, catalogue database, and test client. With Docker and Compose v2 available, run from the checkout
root:

.. code-block:: bash

   docker compose -p openghg-mirror-lab \
       -f tests/integration/irods_mirror/compose.yaml \
       run --build --rm client

Follow the lab README for prerequisites, troubleshooting, reruns, and cleanup
of its named volumes. Use its generated test credentials only for that
disposable zone. No Blue Pebble credentials, resource names, paths, or zone
secrets are needed.

A useful acceptance check establishes all of the following:

* A source object initially has no replica on the consumer resource; a demand
  read creates one with the same catalogue data ID and matching checksum.
* A second read reuses the target replica, and explicitly targeted reads work
  while the source resource is marked unavailable but the catalogue is online.
* A genuine ``read_object`` account can read prepopulated replicas and cannot
  create missing ones. A replication-capable account can populate them.
* Source updates leave pinned readers usable and require new-generation
  objects for newly opened readers. Strict missing-replica reads fail clearly.
* A transfer larger than the server's parallel-transfer threshold succeeds
  through the intended network path, with verified TLS on both servers.

The local lab checks the registered-replica topology. It does not establish
that a particular deployment can reach Blue Pebble, that a site's VPN or SSH
route supports server traffic, or that an operator has approved the mirror
server as a trusted zone server. Validate those deployment conditions separately before
joining an existing zone. Keep private deployment instructions and secrets
outside the repository.
