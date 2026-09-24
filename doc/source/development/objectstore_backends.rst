Configuring ObjectStore backends
================================

OpenGHG selects an ObjectStore implementation for each configured store. Existing
stores continue to use the local TinyDB metadata store and versioned Zarr payloads.
Backend authors can supply a factory that provides the same standardisation,
search, retrieval, and data-management operations with different persistence.

This page describes the factory interface and its configuration. It does not
install a remote backend: preserving a URI in configuration does not itself add
SSH, fsspec, or iRODS transport support.

Select a factory
----------------

Start with the configuration created by :doc:`../install`, then edit the
``object_store`` entry in ``~/.openghg/openghg.conf``. On Windows this file is
under ``LOCALAPPDATA/openghg``. Keep the generated ``user_id`` and
``config_version`` entries.

The following entry illustrates a separately installed backend. Replace the
factory, location, and options with those accepted by that backend:

.. code-block:: toml

   [object_store.remote]
   path = "example://host/catalog"
   permissions = "rw"
   factory = "my_backend:open_store"

   [object_store.remote.options]
   profile = "science"
   credentials_file = "/home/alice/.config/my_backend/credentials"

   [object_store.remote.credentials_env]
   token = "OPENGHG_REMOTE_TOKEN"

``factory`` is an importable ``module:callable``. OpenGHG imports it when the
store is opened. The callable receives ``options`` as keyword arguments.
An absent ``factory`` selects the existing local implementation.

``path`` is the backend's location identifier. For custom factories OpenGHG
preserves it exactly, without resolving it as a local path or checking for a
local Zarr layout. Different configured custom stores must have distinct
locations, including when they use different credentials. Normal workflows
select the configuration name, for example
``search(data_type="surface", store="remote")``.

``credentials_env`` maps factory argument names to environment-variable names.
In this example, the current value of ``OPENGHG_REMOTE_TOKEN`` becomes the
``token`` argument each time the factory is called. An unset variable raises
``ConfigFileError`` before the factory runs. Native credential files, SSH agents,
and backend-specific profiles can instead be handled by the factory itself;
OpenGHG does not interpret the contents of ``options`` or expand their paths.

Option and credential argument names must be distinct and cannot override
``bucket``, ``data_type``, ``mode``, ``skip_keys``, or ``extend_keys``. Configured
read-only custom stores reject write access before the factory runs. The backend
must also enforce ``mode="r"`` in its own mutation methods.

The ``user`` store may use a custom factory. Tutorial mode and helpers that
explicitly return a local user-store path require a local ``user`` entry and
raise ``ObjectStoreError`` otherwise.

Implement the factory
---------------------

Both ``open_object_store`` and ``locking_object_store`` call the selected factory
with these keyword arguments, followed by the configured backend options:

.. code-block:: python

   def open_store(
       *,
       bucket,
       data_type,
       mode="rw",
       skip_keys=None,
       extend_keys=None,
       **backend_options,
   ):
       ...

Return an ObjectStore-compatible object with ``search``, ``retrieve``,
``get_datasource``, ``get_uuids``, ``create``, ``update``, ``delete``, ``close``,
and context-manager methods. The existing ``ObjectStore`` facade in
``openghg.objectstore._objectstore`` combines a ``MetaStore``, a
``DatasourceFactory``, a metadata updater, and document persistence; it can be
reused or subclassed. The backend owns locking and session management.

The returned object must support reads before entering its write context and
repeated entry into that context. Standardisation can process several files
using the same object. Closing a completed write context must persist changes
and release its lock while allowing later operations to acquire resources again.
``close()`` must tolerate repeated calls. A generator-based context manager alone
does not meet this contract.

Datasources returned by ``get_datasource`` or ``retrieve`` must remain usable
after the surrounding ObjectStore context closes. In particular, normal data
retrieval obtains a datasource and subsequently calls ``get_data(version=...)``.
If the backend closes its client session, the datasource must reopen access or
own independent resources. Lazy datasets must also retain access to their data
for as long as they need it.

Persist store documents
-----------------------

Standardisation state and custom metadata-key configuration belong to the
backend. The facade accepts a ``documents`` object implementing
``DocumentStore`` from ``openghg.objectstore._documents``:

.. code-block:: python

   def read(self, key: str) -> dict | None:
       ...

   def write(self, key: str, value: dict) -> None:
       ...

The facade exposes these as ``read_document`` and ``write_document``. Missing
documents return ``None``. Keys are relative logical names; the backend chooses
how to persist them. Local stores retain their existing JSON filenames.

Metadata-key configuration can open a factory with ``data_type=""``. This is a
store-level document handle and must not require a scientific datasource or a
data-type-specific metastore. It must support the same context and permission
rules as other handles. Creating the handle must not recursively look up its own
metadata-key configuration.

Reuse datasource behaviour
--------------------------

``Datasource`` in ``openghg.objectstore._legacy_datasource`` retains the shared
overlap, versioning, metadata, and date-range behaviour. Its persistence hooks
allow a subclass to supply alternative versioned payload storage and datasource
state: ``_create_store``, ``_read_state``, ``_write_state``, ``_delete_state``,
and ``_delete_store_directory``. A subclass that stores client sessions or
other runtime fields must extend ``_runtime_state_keys`` so those fields are
excluded from saved datasource state.

``DataManager.update_attributes`` calls the datasource's ``update_attributes``
method and then ``save``. A backend can implement those methods for its own
payload format without exposing a local Zarr store to the data manager.
Factories should preserve the normal overlap and version-selection semantics;
accepting the factory arguments alone does not establish behavioural parity.

Validate a backend
------------------

Exercise configured standardisation followed by search and retrieval, append
and overlap policies, previous-version retrieval, metadata and attribute edits,
custom metadata keys, deletion, and read-only access. Include a datasource read
after closing its ObjectStore context and several writes through one reused
handle. Test backend failures and simultaneous writers using the backend's
actual persistence and locking mechanisms.

``tests/objectstore/test_factory.py`` checks factory selection, credential
resolution, and configuration errors. It provides a small selection fixture,
not a replacement for the backend's persistence and workflow tests.
