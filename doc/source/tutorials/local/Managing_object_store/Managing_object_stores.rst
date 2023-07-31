.. _managing-object-stores:
Managing object stores
======================

As of OpenGHG 0.6.0 multiple object stores are supported. This means you can read and write to multiple stores as long
as you have permission to do so.

Getting setup
-------------

To create your configuration file use the ``openghg --quickstart`` command line tool.
You'll be asked where you want your object store created, by default this is in your home folder
and you can just press enter if you're happy with the default. This first one is your personal object store,
don't give the path to a shared object store here. By default your local store will have read/write permissions.

.. code-block:: bash

    $ openghg --quickstart

    OpenGHG configuration
    ---------------------

    INFO:openghg.util:We'll first create your user object store.

    Enter path for object store (default /home/gareth/openghg_store):
    Would you like to add another object store? (y/n): n
    INFO:openghg.util:Configuration written to /home/gareth/.config/openghg/openghg.conf

Your configuration file should now look something like this. Note that your ``user_id`` will be different
as this is a string that's randomly generated when you first create your configuration file.

.. code-block:: toml

    user_id = "31f76232-149a-4226-abfb-2788c5fb15d4"
    config_version = "2"

    [object_store.user]
    path = "/home/gareth/openghg_store"
    permissions = "rw"

Multiple stores
---------------

New stores can be easily added using the tool, run ``openghg --quickstart`` again and select yes when asked if you want
to add another store. You can add as many stores as you want, giving a path and the permissions you have to that store.
Note that your write permissions will be checked by OpenGHG.

.. code-block:: bash

    $ openghg --quickstart

    OpenGHG configuration
    ---------------------

    INFO:openghg.util:User config exists at /home/gareth/.config/openghg/openghg.conf, checking...
    INFO:openghg.util:Current user object store path: /home/gareth/openghg_store
    Would you like to update the path? (y/n): n
    Would you like to add another object store? (y/n): y
    Enter the name of the store: group_store
    Enter the object store path: /shared/network/drive/object_store

    You will now be asked for read/write permissions for the store.
    For read only enter r, for read and write enter rw.

    Enter object store permissions: r
    Would you like to add another object store? (y/n): n
    INFO:openghg.util:Configuration written to /home/gareth/.config/openghg/openghg.conf

Your configuration file will now look something like this

.. code-block:: toml

    user_id = "31f76232-149a-4226-abfb-2788c5fb15d4"
    config_version = "2"

    [object_store.user]
    path = "/Users/gar/openghg_store"
    permissions = "rw"

    [object_store.group_store]
    path = "/shared/network/drive/object_store"
    permissions = "r"

With a single object store with read/write permissions any data you standardise with OpenGHG will be added to that
store. When you use the search and retrieve functions both object stores will be searched. We document how to
select which store to write data to in :ref:`addings-obs-data`.

Migrating to the new configuration file
---------------------------------------

If you've used a version of OpenGHG before 0.6.0 you'll need to update your local configuration file to support the new
schema. You can do this easily using the command line tool. It will first ask if you want to update the path
of your local object store, then if you would like to add any new stores. If you answer no to both of these questions
your configuration file will be updated to the new version 2 schema and will work with the latest version of OpenGHG.

.. code-block:: bash

    $ openghg --quickstart

    OpenGHG configuration
    ---------------------

    INFO:openghg.util:User config exists at /home/gareth/.config/openghg/openghg.conf, checking...
    INFO:openghg.util:Current user object store path: /home/gareth/openghg_store
    Would you like to update the path? (y/n): n
    Would you like to add another object store? (y/n): n
    INFO:openghg.util:Configuration written to /home/gareth/.config/openghg/openghg.conf
