===================
Data Specification
===================

Here we set out the specfication for the data we expect for the different storage classes.

ObsSurface
----------

This handles all surface observations. Most data processing is done by ``ObsSurface.readfile`` which handles
all of the data processing itself.

If you need to use functions such as ``ObsSurface.store_data``, these expect data in a specific format.

.. code-block:: python

    data = {
            "site_name": {  "data": xarray.Dataset,
                            "metadata": {"site": "site_code", ...},
                            "attributes": {...}
                         },
            "site_name": {  "data": xarray.Dataset,
                            "metadata": {"site": "site_code", ...},
                            "attributes": {...}
                        },
            }

Each dataset must have a ``time`` variable, the species variable, species_variability and ...
