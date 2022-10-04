=========================
Standardisation functions
=========================

Functions that accept data in specific formats, standardise it to a CF-compliant format and ensure it has the correct metadata attached. The
data returned from these functions is then stored in the object store.

Measurement Standardisation
===========================

These functions cover the four types of measurement we currently support.

Surface measurements
^^^^^^^^^^^^^^^^^^^^

.. autofunction:: openghg.standardise.standardise_surface

Boundary Conditions
^^^^^^^^^^^^^^^^^^^

.. autofunction:: openghg.standardise.standardise_bc

Emissions / Flux
^^^^^^^^^^^^^^^^

.. autofunction:: openghg.standardise.standardise_flux

Footprints
^^^^^^^^^^

.. autofunction:: openghg.standardise.standardise_footprint


Behind the scence these functions use parsing functions that are written specifically for each data type.
Please see the :ref:`Developer API <Developer API>` for these functions.


Metadata
========

.. autofunction:: openghg.standardise.meta.assign_attributes

.. autofunction:: openghg.standardise.meta.get_attributes
