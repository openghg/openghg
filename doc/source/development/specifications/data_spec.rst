===================
Data Specification
===================

Here we set out the specfication for the data we expect for the different storage classes.

Schema validation
-----------------

Each storage class exposes its internal xarray format through ``schema()``.
The returned :class:`openghg.store.DataSchema` records required data variables,
their dimensions, and NumPy data types. For example:

.. code-block:: python

    from openghg.store import Flux

    schema = Flux.schema()
    schema.validate_data(dataset)

Storage classes also provide ``validate_data()`` as a convenient class-level
entry point:

.. code-block:: python

    Flux.validate_data(dataset)

Validation is implemented by ``xarray-validate`` behind the existing
``DataSchema`` API. Schema-library failures are exposed as OpenGHG
``ValidationError`` exceptions, so callers do not need to depend on the
validation library directly. For compatibility, a missing dimension on a
required variable continues to raise ``ValueError``.

When adding or changing an internal format, define required variables and
data types in the storage class's ``schema()`` method:

.. code-block:: python

    import numpy as np

    from openghg.store import DataSchema

    DataSchema(
        data_vars={"example": ("time",)},
        dtypes={"example": np.floating, "time": np.datetime64},
    )

Extra variables remain allowed, and dimensions listed for a required variable
must be present but may appear in a different order. Dtype constraints for
coordinates remain optional when the coordinate is absent, matching the
historical OpenGHG schema behaviour.

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
