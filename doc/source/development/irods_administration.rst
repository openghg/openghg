===============================
Administer a shared iRODS store
===============================

This guide is for the operator of an existing iRODS service who wants to give
named users access to an experimental OpenGHG store. It covers accounts, shared
collection permissions, and operational checks. See :doc:`irods_prototype` for
client configuration and scientific workflows.

Prepare the service
===================

Install the OpenGHG ``irods`` extra in the administrator's Python environment.
The commands below use a zone administrator's iRODS environment file and native
authentication. Account and group creation require administrator privileges;
ordinary readers and writers should receive ``rodsuser`` accounts.

The operator must already have provisioned an iRODS server, catalogue database,
and registered storage resource. The helper does not install or launch these
services. Follow the `iRODS installation documentation
<https://irods.org/download/>`__ and the Python client's
`secure connection instructions
<https://github.com/irods/python-irodsclient#establishing-a-secure-connection>`__.
Configure encrypted client connections and certificate verification before
sharing native credentials over a network.

Keep the database, authentication files, server keys, and physical resource
vault accessible only to the service operator. A shared filesystem directory
can contain connection instructions and non-secret configuration; it should
not expose the vault for direct editing. Filesystem access neither creates an
iRODS account nor grants access to an iRODS collection. A Unix group and an
iRODS group are separate membership lists.

Create accounts and a shared collection
=======================================

Choose a dedicated logical collection and two dedicated iRODS group names.
The example uses ``/yourZone/projects/observations``, ``observations_readers``,
and ``observations_writers``. Replace them consistently with your own names.
Missing parent collections are created as ordinary collections.

Create each local-zone account with a password prompt:

.. code-block:: bash

   export IRODS_ENVIRONMENT_FILE="$HOME/.irods/admin_environment.json"
   python -m openghg.objectstore.irods_admin create-user alice
   python -m openghg.objectstore.irods_admin create-user bob

The password is entered without terminal echo and is never accepted as a
command-line argument. An existing ordinary account is left unchanged;
``create-user`` does not reset its password. Use the separate ``set-password``
command for an intentional reset or to finish provisioning after a password-setting
failure. Deliver initial credentials through
your normal private channel, and keep each user's authentication file private.
Do not distribute the administrator's environment or authentication file.

Create the store and assign roles:

.. code-block:: bash

   python -m openghg.objectstore.irods_admin setup \
       /yourZone/projects/observations \
       --readers observations_readers --writers observations_writers \
       --reader alice --reader bob --writer alice

Alice can read and write; Bob can read. Setup creates missing groups and the
store collection, grants group ACLs recursively to existing descendants, and
enables inheritance for future descendants. It also grants the groups enough
nonrecursive access to ancestors for the backend's catalogue checks. Ancestor
access reveals their catalogue entries, without granting access to unrelated
descendants.

Repeating setup with the same collection and group names is additive and
idempotent. It does not remove members omitted from a later command. An
existing nonempty collection without the helper's setup marker requires
``--adopt-existing``. Inspect its contents and ACLs before adopting it: setup
adds role permissions and does not remove existing grants. Use a new dedicated
collection if its current access policy is unsuitable.

The roles use the following server permissions:

.. list-table:: Shared store roles
   :header-rows: 1
   :widths: 15 25 60

   * - Role
     - iRODS permission
     - Effect
   * - Reader
     - ``read_object``
     - Search catalogue records and download data; cannot modify them.
   * - Writer
     - ``own``
     - Read, create, update, and delete store data and metadata, including
       material created by another writer; also change ACLs on the shared tree.

The helper gives trusted collaborators ``own`` throughout this store, including
permission to maintain ACLs and perform deliberate physical cleanup. This also
supports the earlier mutable backend, whose trash-based deletion required
ownership on the tested iRODS 5.0.2 server. The current backend's logical deletion
publishes a tombstone and retains generations for existing readers; it does not
move payloads to trash. See :doc:`irods_publication` for retention boundaries.
A less privileged ingestion role requires a separately tested server policy;
this helper does not configure that role.

Configure and verify each client
================================

Give each user the service address, port, zone, logical collection, trusted CA
certificate if needed, and their own account credentials. Configure the
ObjectStore factory as shown in :doc:`irods_prototype`, with
``permissions = "r"`` for readers and ``"rw"`` for writers. Give every user a
private local cache directory. Server ACLs enforce access even if a reader
changes the local setting to ``"rw"``.

Read-only remote use needs the same authenticated connection as local use.
Where the service is reachable through an SSH host, a loopback-forwarded
connection can be established with:

.. code-block:: bash

   ssh -N -L 11247:127.0.0.1:11247 your-login-host

This example assumes the iRODS service is on that host's loopback port 11247.
Use the operator's actual route and port. The client connects to the forwarded
endpoint; its TLS settings must still verify the presented certificate and
the hostname or IP address used for that endpoint. Do not disable verification
to fix a mismatch. Opening the port does not grant iRODS access. The current
OpenGHG transport uses one transfer stream, so its tested operations do not
need a separate parallel-transfer port range. Laptop routing still needs an
end-to-end check in your deployment.

As each intended user, verify that search returns a known dataset and loading a
small selection succeeds. Verify a reader cannot write using a separate
authenticated connection; running a read-only Python object under the
administrator's account does not test server access control.

Manage membership and revocation
================================

Use explicit membership commands after initial setup:

.. code-block:: bash

   python -m openghg.objectstore.irods_admin add-member observations_writers bob
   python -m openghg.objectstore.irods_admin remove-member observations_writers bob
   python -m openghg.objectstore.irods_admin access /yourZone/projects/observations

The first command promotes Bob to writer; the second removes that group
membership. The access report shows the root's ACLs and configured role
memberships. It is not a recursive audit of every descendant's effective
permissions. Removing a writer from its group does not remove ownership or
direct grants on objects it created, membership in other groups, or issued
tickets. Audit those paths when revoking a former writer's access. Setup does
not attempt to rewrite ownership or infer a site's membership policy.

Removing a reader from the reader group removes that route to future access.
Check with a fresh authenticated connection, and account for other grants and
existing transfers. Downloaded files and computed arrays already belong to the
recipient; revocation cannot recall them. The backend rechecks catalogue
visibility when it uses its verified cache, but recipients can still read
previously downloaded bytes directly from disk.

Keep the service recoverable
============================

Back up the catalogue database together with the resource vaults and the
configuration needed to restore them. A replica or client cache alone does not
back up the catalogue. Record the service owner, supported start/stop process,
log locations, certificate renewal procedure, account handover, and restore
procedure in deployment notes outside the repository if they contain internal
details.

Before removing an abandoned ``.openghg-write-lock`` collection, establish
that its writer has stopped and inspect the interrupted operation. The admin
helper deliberately has no automatic lock-breaking command. Normal membership
changes do not require deleting the lock or changing datasource metadata.

Test the access model
=====================

The unit checks run without a server:

.. code-block:: bash

   python -m pytest tests/objectstore/test_irods_admin.py

The opt-in access check creates temporary users, groups, and a child collection
in an existing dedicated test root. It needs a native-authenticated zone
administrator and removes the temporary accounts and data afterwards:

.. code-block:: bash

   export IRODS_ENVIRONMENT_FILE="$HOME/.irods/admin_environment.json"
   export OPENGHG_IRODS_TEST_COLLECTION='/yourZone/home/admin/openghg-tests'
   export OPENGHG_IRODS_ADMIN_TESTS=1
   python -m pytest tests/objectstore/test_irods_access.py

It exercises independent reader, writer, and unprivileged accounts; inherited
access to newly created data; reader write rejection; writer changes to another
user's data; and reader membership revocation. Run ordinary scientific workflow
checks separately as described in :doc:`irods_prototype`.
