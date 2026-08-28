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

Unit requirements
~~~~~~~~~~~~~~~~~

Schemas declare units only for variables whose physical meaning is known. The
``units`` mapping requires a unit equivalent to the declared unit; a ``None``
value requires a non-empty units string without constraining its dimension.
``units_compatible`` accepts scaled units with the same dimensionality:

.. code-block:: python

    DataSchema(
        data_vars={"flux": ("time", "lat", "lon")},
        units={"lat": "degrees_north", "lon": "degrees_east"},
        units_compatible={"flux": "mol m-2 s-1"},
    )

The main surface-observation variable uses the non-empty form because surface
data includes mole fractions, isotopes, particulates, and other dimensions.
Observation cardinalities such as ``number_of_observations`` are deliberately
not assigned units and must not be converted with the observation signal.

Flux, footprint, and boundary-condition schemas use dimensional constraints.
Latitude, longitude, height, and numeric back-time coordinates are also
validated where present in those formats. Datetime coordinates are excluded:
xarray moves their CF units into the encoding when it decodes them, and Pint
handles datetime coordinates without a units attribute.

Unit parsing uses OpenGHG's CF-aware Pint registry, so scaled mole fractions
such as ``ppm``, ``ppb``, and ``1e-9`` and CF spellings such as
``degrees_north`` are handled consistently. Add requirements to the named
physical variables in a storage-class schema; do not use a wildcard merely to
require units on every data variable.

Other attribute requirements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``required_attrs`` to require non-empty string attributes on named data
variables or coordinates, and ``dataset_attrs`` for Dataset-level attributes:

.. code-block:: python

    DataSchema(
        data_vars={"example": ("time",)},
        required_attrs={"example": {"long_name", "source"}},
        dataset_attrs={"species"},
    )

Attribute requirements should describe stable parts of an internal data format,
not metadata that is only available from some source formats. Surface signals
require ``long_name``; fluxes require ``source`` and ``species``; footprint and
boundary-condition signals require ``long_name``. Add or normalize these
attributes in the standardizer before schema validation rather than silently
adding them in the validator.

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
