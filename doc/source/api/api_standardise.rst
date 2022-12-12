===========
Standardise
===========

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


Helpers
=======

Some of the functions above require quite specific arguments as we must ensure all metadata attriuted to data is as correct as possible.
These functions help you find the correct arguments in each case.

.. autofunction:: summary_source_formats

.. autofunction:: summary_site_codes


Behind the scences these functions use parsing functions that are written specifically for each data type.
Please see the :ref:`Developer API <Developer API>` for these functions.
