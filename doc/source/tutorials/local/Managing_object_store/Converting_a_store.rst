Converting an object store
==========================

In this tutorial we'll convert an object store from the old-style NetCDF format
to the new Zarr based object store format.

In this example we have an old-style object store at ``/home/gareth/openghg_store`` and we want to convert it to the new Zarr format.
To perform the conversion we need the path of the old old-style store and the name of the new store to write to.

Let's first add a new object store to our ``openghg.conf`` using ``openghg --quickstart``.

.. code-block:: bash

    $ openghg --quickstart

    OpenGHG configuration
    ---------------------

    INFO:openghg.util:User config exists at /home/gareth/.openghg/openghg.conf, checking...             _user.py:91
    INFO:openghg.util:Current user object store path: /home/gareth/openghg_store                       _user.py:102
    Would you like to update the path? (y/n): n
    Would you like to add another object store? (y/n): y
    Enter the name of the store: openghg_store_zarr
    Enter the object store path: /home/gareth/openghg_store_zarr

    You will now be asked for read/write permissions for the store.
    For read only enter r, for read and write enter rw.

    Enter object store permissions: rw
    Would you like to add another object store? (y/n): n
    INFO:openghg.util:Configuration written to /home/gareth/.openghg/openghg.conf


Now we have a new object store called ``openghg_store_zarr`` in our configuration file and the path of that object
store is ``/home/gareth/openghg_store_zarr``. We're now ready perform the conversion.

.. code-block:: python

    from openghg.store.storage import convert_store

    old_store = "/home/gareth/openghg_store"
    new_store = "openghg_store_zarr"

    convert_store(path_in=old_store, store_out=new_store)

The ``convert_store`` function iterates over each of the data storage classes (``Footprints``, ``ObsSurface`` etc) and adds the data to the new object store.
It does this by reading the metadata from the the metadata store and passing the data stored as NetCDF files to the
appropriate ``standardise_*`` functions.

The conversion process can take some time depending on the size of the object store. The conversion process is not
atomic and if the process is interrupted the new object store may be broken. If the conversion
process is interrupted it is recommended to delete the new object store and start the conversion process again.

.. NOTE::

    We recommend that object stores are populated using the original data, this will result in a more
    consistent store. We recommend using the conversion process only when necessary.

The conversion function will attempt to catch errors are they are raised during the conversion process.
You may see lines such as ``Error standardising record <uuid>: Codec does not support buffers of > 2147483647 bytes``
in the log output. These errors mean that the chunks being written to the object store are too large and need
to be reduced through the use of chunking.

To do this we can pass a chunks dictionary and the name of the data type that was being converted during conversion to the ``convert_store`` function.

.. code-block:: python


    from openghg.store.storage import convert_store

    old_store = "/home/gareth/openghg_store"
    new_store = "openghg_store_zarr"

    convert_store(path_in=old_store, store_out=new_store, to_convert=["footprints"], chunks={"time": 24})

You may need to experiment with the chunk sizes to find the optimal size for the data being converted.
