Managing and deleting data
==========================

Sometimes you might want to modify some metadata after running the data
through the standardisation scripts. After the standardisation process
the metadata associated with some data can still be edited. This can
save time if the data standardisation process is quite time consuming.
Data can also be deleted from the object store. We use some lower level functions
in this tutorial that you may not have encountered before. Take time to understand
what they are doing and check each operation carefully.

.. warning::
    The functionality exposed by the ``DataManager`` class could lead to data loss.
    Please use it carefully.

This tutorial will not work with the normal ``use_tutorial_store`` command. We need to manually backup
the OpenGHG configuration file and create a new one.

.. code:: bash

    mv ~/.openghg/openghg.conf ~/.openghg/openghg.conf.bak
    openghg --quickstart

    OpenGHG configuration
    ---------------------

    INFO:openghg.util:We'll first create your user object store.

    Enter path for your local object store (default /home/gareth/openghg_store): /home/gareth/testing_store
    Would you like to add another object store? (y/n): n
    INFO:openghg.util:Configuration written to /home/gareth/.config/openghg/openghg.conf


We'll first add some footprint data to the object store, we'll explicitly pass in the name of the store we want to write
the data to here, the "user" store.

.. code:: python

    from openghg.store import data_manager
    from openghg.standardise import standardise_footprint
    from openghg.tutorial import retrieve_example_data

    tac_fp_inert = "https://github.com/openghg/example_data/raw/main/footprint/tac_footprint_inert_201607.tar.gz"
    tac_inert_path = retrieve_example_data(url=tac_fp_inert)[0]

    site = "TAC"
    inlet = "100m"
    domain = "EUROPE"
    model = "NAME"

    store = "user"

    standardise_footprint(filepath=tac_inert_path, site=site, inlet=inlet, domain=domain, model=model, store=store)

Now we're ready to retrieve the metadata from the object store and create a ``DataManager`` object. Again we have to
pass in the name of the store.

.. note::
    You can only pass in the name of a store to which you have write access. If you don't have
    the correct permissions an ``ObjectStoreError`` will be raised.

.. code:: python

    dm = data_manager(data_type="footprints", site="TAC", height="100m", store="user")

.. code:: python3

    dm.metadata

We want to update the model name so we'll use the ``update_metadata``
method of the ``DataManager`` object. To do this we need to take the
UUID of the Datasource returned by the ``data_manager`` function,
this is the key of the metadata dictionary.

   **NOTE:** Each time an object is added to the object store it is
   assigned a unique id using the Python uuid4 function. This means any
   UUIDs you see in the documentation won't match those created when you
   run these tutorials.

For the purposes of this tutorial we take the first key from the
metadata dictionary. We can do this only because we've checked the
dictionary and seen that only one key exists. It also means you can run
through this notebook and it should work without you having to modify
it. But be careful, if the dictionary contains more than one key,
running the cell below might not result in the UUID you want. Each time
you want to modify the data **copy and paste** the UUID and **double
check** it.

.. code:: python

    uuid = next(iter(dm.metadata))

.. code:: python

    updated = {"model": "new_model"}

    dm.update_metadata(uuid=uuid, to_update=updated)


When you run ``update_metadata`` the internal store of metadata for each
``Datasource`` is updated. If you want to **really** make sure that the
metadata in the object store has been updated you can run ``refresh``.

.. code:: python

    dm.refresh()

.. code:: python

    metadata = dm.metadata[uuid]

And check the model has been changed.

.. code:: python

    metadata["model"]

Deleting keys
-------------

Let's accidentally add too much metadata for the footprint and then
delete.

.. code:: python

    excess_metadata = {"useless_key": "useless_value"}
    dm.update_metadata(uuid=uuid, to_update=excess_metadata)

.. code:: python

    dm.metadata[uuid]["useless_key"]


Oh no! We've added some useless metadata, let's remove it.

.. code:: python

    to_delete = ["useless_key"]
    dm.update_metadata(uuid=uuid, to_delete=to_delete)


And check if the key is in the metadata:

.. code:: python

    "useless_key" in dm.metadata[uuid]

Restore from backup
-------------------

If you've accidentally pushed some bad metadata you can fix this easily
by restoring from backup. Each ``DataManager`` object stores a backup of
the current metadata each time you run ``update_metadata``. Let's add
some bad metadata, have a quick look at the backup and then restore it.
We'll start with a fresh ``DataManager`` object.

.. code:: python

    dm = data_manager(data_type="footprints", site="TAC", height="100m", store="user")

.. code:: python

    bad_metadata = {"domain": "neptune"}
    dm.update_metadata(uuid=uuid, to_update=bad_metadata)

Let's check the domain

.. code:: python

    dm.metadata[uuid]["domain"]

Using ``view_backup`` we can check the different versions of metadata we
have backed up for each ``Datasource``.

.. code:: python

    dm.view_backup()

To restore the metadata to the previous version we use the ``restore``
function. This takes the UUID of the datasource and optionally a version
string. The default for the version string is ``"latest"``, which is the
version most recently backed up. We'll use the default here.

.. code:: python

    dm.restore(uuid=uuid)

Now we can check the domain again

.. code:: python

    dm.metadata[uuid]["domain"]

To really make sure we can force a refresh of all the metadata from the
object store and the ``Datasource``.

.. code:: python

    dm.refresh()

Then check again

.. code:: python

    dm.metadata[uuid]["domain"]

Multiple backups
----------------

The ``DataManager`` object will store a backup each time you run
``update_metadata``. This means you can restore any version of the
metadata since you started editing. Do note that the backups, currently,
only exist in memory belonging to the ``DataManager`` object.

.. code:: python

    more_metadata = {"time_period": "1m"}
    dm.update_metadata(uuid=uuid, to_update=more_metadata)

We can view a specific metadata backup using the ``version`` argument.
The first version is version 1, here we take a look at the backup made
just before we made the update above.

.. code:: python

    backup_2 = dm.view_backup(uuid=uuid, version=2)

.. code:: python

    backup_2["time_period"]

Say we want to keep some of the changes we've made to the metadata but
undo the last one we can restore the last backup. To do this we can pass
“latest” to the version argument when using ``restore``.

.. code:: python

    dm.restore(uuid=uuid, version="latest")

.. code:: python

    dm.metadata[uuid]["time_period"]

We're now back to where we want to be.

Deleting data
-------------

To remove data from the object store we use ``data_manager``
again

.. code:: python

    dm = data_manager(data_type="footprints", site="TAC", height="100m", store="user")

.. code:: python

    dm.metadata

Each key of the metadata dictionary is a Datasource UUID. Please make
sure that you **double check the UUID** of the Datasource you want to
delete, this operation cannot be undone! Also remember to change the
UUID below to the one in your version of the metadata.

.. code:: python

    uuid = "13fd70dd-e549-4b06-afdb-9ed495552eed"

.. code:: python

    dm.delete_datasource(uuid=uuid)

To make sure it's gone let's run the search again

.. code:: python

    dm = data_manager(data_type="footprints", site="TAC", height="100m", store="user")

.. code:: python

    dm.metadata

An empty dictionary means no results, the deletion worked.

Tidy up
-------

To restore your old OpenGHG configuration file run

.. code:: bash

    mv ~/.openghg/openghg.conf.bak ~/.openghg/openghg.conf
