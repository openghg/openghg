=========
Transform
=========

Functions that can convert from underlying databases or model outputs into the standardised OpenGHG format.
This could include, for example, creating a Flux file for a limited domain based on data from the EDGAR database.
In constrast to standardisation functions, this will usually include some amount of transformation such as selection and/or regridding.

Regridding
==========

.. autofunction:: openghg.transform.regrid_uniform_cc

Emissions
=========

Transform emissions data

.. autofunction:: openghg.transform.emissions.parse_edgar
