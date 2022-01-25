=========================
Standardisation functions
=========================

Functions that accept data in specific formats, standardise it to a CF-compliant format and ensure it has the correct metadata attached. The
data returned from these functions is then stored in the object store.

Surface
=======

These functions take surface measurement data, they are called by the ``ObsSurface`` class.


.. autofunction:: openghg.standardise.surface.parse_aqmesh


.. autofunction:: openghg.standardise.surface.parse_beaco2n


.. autofunction:: openghg.standardise.surface.parse_btt


.. autofunction:: openghg.standardise.surface.parse_cranfield


.. autofunction:: openghg.standardise.surface.parse_crds


.. autofunction:: openghg.standardise.surface.parse_eurocom


.. autofunction:: openghg.standardise.surface.parse_gcwerks


.. autofunction:: openghg.standardise.surface.parse_noaa


.. autofunction:: openghg.standardise.surface.parse_npl


.. autofunction:: openghg.standardise.surface.parse_tmb

Metadata
========

.. autofunction:: assign_attributes

.. autofunction:: get_attributes

.. autofunction:: surface_standardise
