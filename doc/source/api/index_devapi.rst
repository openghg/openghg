===========================
Developer API documentation
===========================

The functions and methods documented in this section are the internal workings of the OpenGHG library. They are subject to change
without warning due to the early stages of development of the project. Normal users should not use any of the function shown here directly
as their functionality may change. 

modules
=======

These classes are used for the processing of data by the ``ObsSurface`` processing class. 

* Base class

:class:`~openghg.modules.BaseModule`
    Base class which the other core processing modules inherit (currently only ObsSurface)

* Processing classes

:class:`~openghg.modules.CRANFIELD`
    For processing data from Cranfield
:class:`~openghg.modules.CRDS`
    For processing data from CRDS (cavity ring-down spectroscopy) data from the DECC network.

:class:`~openghg.modules.EUROCOM`
    For processing data from the EUROCOM network
:class:`~openghg.modules.GCWERKS`
    For processing data in the form expected by the GCWERKS package
:class:`~openghg.modules.ICOS`
    For processing data from the ICOS network
:class:`~openghg.modules.NOAA`
    For processing data from the NOAA network
:class:`~openghg.modules.THAMESBARRIER`
    For processing data from the Thames Barrier measurement sites

* Datasource

The Datasource is the smallest data provider within the OpenGHG topology. A Datasource represents a data provider such as an instrument
measuring a specific gas at a specific height at a specific site. For an instrument measuring three gas species at an inlet height of 100m
at a site we would have three Datasources.

:class:`~openghg.modules.Datasource`
    Handles the storage of data, metadata and version information for measurements
