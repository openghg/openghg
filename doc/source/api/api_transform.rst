=========
Transform
=========

Functions that can convert from underlying databases or model outputs into the standardised OpenGHG format.
This could include, for example, creating a Flux file for a limited domain based on data from the EDGAR database.
In constrast to standardisation functions, this will usually include some amount of transformation such as selection and/or regridding.

Regridding
==========

.. autofunction:: openghg.transform.regrid_uniform_cc

Transform entry points
======================

Transform emissions or boundary-condition data and store the result.

.. autofunction:: openghg.transform.transform_flux_data

.. autofunction:: openghg.transform.transform_bc_data

Database parsers
================

.. autofunction:: openghg.transform.flux.parse_edgar

.. autofunction:: openghg.transform.boundary_conditions.parse_cams
