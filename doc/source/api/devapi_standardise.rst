==================
Standardise - data
==================

Each of these functions parses a specific type of data file and returns a dictionary containing the data and metadata.

Surface observations
^^^^^^^^^^^^^^^^^^^^

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


Column data
^^^^^^^^^^^

.. autofunction:: openghg.standardise.column.parse_openghg


Emissions / flux
^^^^^^^^^^^^^^^^

.. autofunction:: openghg.standardise.column.parse_openghg


Metadata
^^^^^^^^

These ensure the metadata and attributes stored with data are correct.

.. autofunction:: assign_attributes

.. autofunction:: get_attributes

.. autofunction:: assign_flux_attributes

.. autofunction:: define_species_label

.. autofunction:: metadata_default_keys

.. autofunction:: sync_surface_metadata
