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

.. code:: toml
    user_id = "6dea888d-840a-4cd6-974e-f9888c4b7be3"

    [object_store]
    local_store = "/home/gareth/openghg_store"

To add a new object store you can use the ``--add-store`` argument

.. code:: shell

    openghg --add-store
    Please enter the name of the store you'd like to add: group_store
    Please enter the path of the object store: /network_share/group_share/group_object_store

    You've added an object store {group_store: "/network_share/group_share/group_object_store"}

    Configuration updated successfully and written to ~/.config/openghg/openghg.conf


QUESTION - how should we set a precedent for the object stores? Do we need to?
