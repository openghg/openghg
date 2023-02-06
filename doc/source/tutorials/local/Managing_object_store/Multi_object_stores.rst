Using multiple object stores
============================

When run locally, OpenGHG has the ability to access multiple object stores. This means
you can store data wherever you select. When data is to be added to the object store OpenGHG
will ask you which store you want to add it to.

1. Adding an object store
-------------------------

If you've run through the Quickstart tutorial you'll have setup a local configuration file
using the ``openghg --quickstart`` command line interface.

Let's say we start with a configuration file that looks like this

.. code-block:: toml
    user_id = "6dea888d-840a-4cd6-974e-f9888c4b7be3"

    [object_store]
    local_store = "/home/gareth/openghg_store"

To add a new object store you can use the ``--add-store`` argument

.. code-block:: console

    openghg --add-store
    Please enter the name of the store you'd like to add: group_store
    Please enter the path of the object store: /network_share/group_share/group_object_store

    You're added an object store named group_store at /network_share/group_share/group_object_store
    Is this correct? (y/n):

    Configuration updated successfully and written to ~/.config/openghg/openghg.conf

QUESTION - how should we set a precedent for the object stores? Do we need to?

Now that we've got the extra object store added our ``openghg.conf`` should look like this:

.. code-block:: toml
    user_id = "6dea888d-840a-4cd6-974e-f9888c4b7be3"

    [object_store]
    local_store = "/home/gareth/openghg_store"
    group_store = "/network_share/group_share/group_object_store"

Now we can look at adding some data to that object store

2. Adding data to a specific store
----------------------------------

In the :ref:`Adding observation data<Adding observation data>` tutorial we added data to our local object
store. Here we'll tell the standardisation functions where to want to store the data. To do this we used
the ``object_store`` argument. We'll use :ref:`standardise_surface<standardise_surface>` to process the data
and :ref:`retrieve_example_data<retrieve_example_data>` to retrieve some example data from our tutorial
data repository.

QUESTION - what should be the default behaviour? This will be different locally and in the cloud.

.. code-block:: ipython3

    In [1]: from openghg.standardise import standardise_surface

    In [2]: from openghg.tutorial import retrieve_surface_example

    In [4]: tac_paths = retrieve_surface_example

    In [4]: result = standardise_surface(filepaths=tac_paths, source_format="CRDS", site="tac", network="DECC", object_store="group_store")

    In [4]: result

Here we can see the result of processing the data and a key "object_store" : "group_store" telling us that the
data has been added to the correct store.

3. Searching for data
---------------------

By default the search functions will look for data in each object store that is listed in your
``openghg.conf`` file.

Design - can remove this
^^^^^^^^^^^^^^^^^^^^^^^^

It does this by applying the underlying search function to each of the metadata stores.
These metadata stores are loaded from fixed paths within each object store and the
search function is applied to it.

So

.. code-block:: python

    # Load in the object stores, this returns a list of object stores from the config file
    object_stores = get_object_stores()
    # The user facing functions

    for metastore in metastores:
