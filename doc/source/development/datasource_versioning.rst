============================================
Edit a datasource and commit a saved version
============================================

Use ``DataManager.datasource(uuid)`` when changing data that already exist in an
object store. The handle identifies both the selected UUID and its configured
store. Standardisation remains the entry point for parsing new source files;
an editor works with an already prepared Xarray Dataset.

An editor stages changes privately. Only ``commit()`` publishes a new version.
Several operations can produce one saved version, and leaving either context
without committing discards the pending changes.

Update existing data
====================

This example corrects one observation in an existing surface datasource. Select
a store and search terms appropriate for your data; narrow the search until it
selects exactly the intended datasource.

.. code-block:: python

   from openghg.dataobjects import data_manager

   manager = data_manager(
       data_type="surface", store="user", site="tac", species="co2"
   )
   if len(manager.metadata) != 1:
       raise ValueError("Select exactly one datasource before editing")
   uuid = next(iter(manager.metadata))

   with manager.datasource(uuid) as source:
       with source.begin_edit(base="latest") as edit:
           correction = edit.get_data().isel(time=slice(0, 1)).load()
           correction["mf"] = correction["mf"] + 0.1
           edit.update(correction)
           edit.update_attributes(to_update={"comment": "Calibration corrected"})
           version = edit.commit(message="Correct the first observation")
       saved = source.get_data(version=version)

   # Saved reads remain usable after the writer context has closed.
   saved.load()

``source`` remains readable after the manager context exits, but further writes
require a fresh ``manager.datasource(uuid)`` context. The manager refreshes only
that UUID from the same backend after the context finishes. Searchable
descriptors are not copied into the datasource by opening this handle.

Choose the operation
====================

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Operation
     - Effect in the working dataset
   * - ``append(data)``
     - Insert timestamps absent from the base. Any overlap rejects the operation.
   * - ``update(data)``
     - Replace values at existing timestamps. Unknown timestamps reject it.
   * - ``upsert(data)``
     - Update matching timestamps and insert new timestamps.
   * - ``replace(data)``
     - Replace the whole working dataset, including its attributes.
   * - ``update_attributes(...)``
     - Change global or selected variable attributes in this working version.
   * - ``get_data()``
     - Return a lazy preview of the unpublished working dataset.

``update`` and ``upsert`` preserve omitted timestamps. A supplied NaN replaces
the existing value; it does not mean "leave unchanged". There is no range
replacement operation: deleting timestamps missing from an incoming time range
is deferred. ``append`` uses the existing Store insertion and overlap rules;
it can fill gaps as well as extend the endpoints.

Incoming attributes override matching global and variable attributes during
append, update, and upsert; unmentioned attributes are retained. Use
``update_attributes(data_vars=..., update_global=..., to_update=...,
to_delete=...)`` for explicit attribute edits. It has no ``version`` parameter:
the editor's chosen base determines what is edited. Searchable descriptors
remain separate and use ``manager.update_metadata(...)``. Descriptor edits are
not part of an editor commit and do not become versioned with the payload.

``begin_edit(base="latest")`` starts from the latest saved version;
``base="v1"`` starts from that saved version, and ``base=None`` starts empty.
A successful commit returns a new monotonically allocated ``vN`` label and
records its base, timestamp, and optional message. It never changes the base
version. There is one active editor per handle. A failed data operation aborts
the editor; start another editor to retry. Committing an untouched editor raises
``ValueError`` without creating a version or changing the datasource policy.
Calling a data operation counts as a change even when the values are identical;
there is no full-dataset equality or hashing pass.

Working previews are valid until the next mutation or abort. A mutation can
consume a lazy preview safely: it writes a fresh working payload while reading
the previous one, without eagerly loading the full dataset into memory. Local
and iRODS backends currently make an extra full copy when a mutation follows an
exposed preview. A batch without exposed previews copies its base only once.
Load a preview before its lifetime ends if you need its values independently.
A preview followed directly by a successful commit, with no intervening
mutation, points to the saved payload. Committed reads from
``source.get_data()`` remain independent snapshots. Closing a returned Xarray
Dataset does not close the manager.

Migrate legacy workflows deliberately
=====================================

Existing datasources retain their legacy ``if_exists``, ``new_version``, and
``save_current`` behavior until the first successful explicit commit. That
commit durably opts the datasource into immutable saved versions. Existing
versions are retained. Legacy mutation and save methods then raise an error
instructing the caller to use ``begin_edit`` and ``commit``; they do not silently
reinterpret old flags. Previously loaded legacy handles also cannot bypass the
policy after opt-in.

Update every writer of an opted-in datasource to use the editor. For example,
replace a loop that repeatedly standardises corrections into the same
mutable version with one editor, several ``update`` or ``upsert`` calls, and
one final commit. Version deletion, datasource deletion, and garbage collection
for opted-in datasources are deferred. Full copies of unchanged data may still
be stored: this API does not introduce chunk sharing or an Icechunk dependency.

Backend and connection lifetime
================================

The manager uses the configured ObjectStore factory and holds its existing
writer context throughout editing and publication. Backends supply payload
staging and publication hooks. Committing one datasource is not a transaction
across multiple datasources, descriptors, or store-level documents. Local
publication uses immutable payload locations and an atomically replaced state
file; failure recovery and concurrency still depend on using the supported
backend writer context.

The draft iRODS work in `PR #1752 <https://github.com/openghg/openghg/pull/1752>`_
and `PR #1753 <https://github.com/openghg/openghg/pull/1753>`_ establishes backend
factories, immutable generations, stale-publication checks, and deferred session
lifetimes. A lazy iRODS read pins generation paths and reconnects through its
configured session factory when needed. If an ``IRODSObjectStore`` is instead
constructed with a borrowed session, that session remains caller-owned and must
stay open for subsequent lazy reads; closing the store does not close it.
The current iRODS transport requires zarr-python 2 and Zarr format 2. Local
Zarr 3 support does not enable the iRODS transport under zarr-python 3.
Read permissions, access to the catalogue, and
retained generations remain necessary. There is no offline-cache guarantee,
reader lease, or automatic reclamation. See :doc:`irods_publication` for those
backend-specific publication and recovery boundaries.

Requirements and remaining work
================================

These issue links record the source of the requirements. "Delivered" below
means covered by this explicit-edit workflow, not that the entire linked issue
is resolved.

.. list-table::
   :header-rows: 1
   :widths: 17 43 40

   * - Requirement
     - Delivered behavior
     - Deferred or unchanged
   * - `#1554 <https://github.com/openghg/openghg/issues/1554>`_
     - Store-bound Datasource handle for editing existing data.
     - Arbitrary public Store selection and index-option configuration.
   * - `#1650 <https://github.com/openghg/openghg/issues/1650>`_
     - Raw datasource loading avoids persisting merged search descriptors.
     - Broader metadata ownership refactor; versioned descriptor transactions.
   * - `#881 <https://github.com/openghg/openghg/issues/881>`_
     - Explicit insert/update/upsert behavior, NaN overwrite, retained omitted timestamps.
     - Range replacement with explicit or inferred deletion bounds.
   * - `#748 <https://github.com/openghg/openghg/issues/748>`_
     - Shared backend writer context and explicit publication boundary.
     - Multi-datasource transactions and automatic conflict merging.
   * - `#591 <https://github.com/openghg/openghg/issues/591>`_
     - Data operation and version publication are separate decisions.
     - Legacy flags retain their existing behavior until explicit opt-in.
   * - `#774 <https://github.com/openghg/openghg/issues/774>`_
     - Several edits can publish one version instead of one per input file.
     - Existing standardisation loops are not automatically rewritten.
   * - `#923 <https://github.com/openghg/openghg/issues/923>`_
     - Explicit attribute preservation and editing in working versions.
     - General provenance or scientific-release metadata model.
   * - `#1550 <https://github.com/openghg/openghg/issues/1550>`_
     - Worked example of correcting already stored data above.
     - Availability and update-preview plotting from #1551.
   * - `#597 <https://github.com/openghg/openghg/issues/597>`_
     - Immutable version identity and an explicit commit point.
     - Chunk sharing, retention policy, physical reclamation.
   * - `#1755 <https://github.com/openghg/openghg/issues/1755>`_
     - Backend-independent edit and publication contract for a future Icechunk prototype.
     - Icechunk integration, migration, and measured storage savings.

The manager regressions in ``tests/dataobjects/test_datasource_handle.py``
cover identical UUIDs in distinct stores, raw metadata provenance, configured
factory dispatch, permissions, discarded edits on normal and exceptional exits,
and lazy saved reads after context exit and a later commit. Editor and backend
tests separately cover operation semantics, immutable saved versions, stale
writes, and publication failure boundaries.
