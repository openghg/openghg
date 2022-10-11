=============
Developer API
=============

The functions and methods documented in this section are the internal workings of the OpenGHG library. They are subject to change
without warning due to the early stages of development of the project.

.. warning:: Normal users should not use any of the functions shown here directly as they may be removed or their functionality may change.

Standardisation
===============

Surface measurements
^^^^^^^^^^^^^^^^^^^^

These functions take surface measurement data and standardise it for storage in the object store. They ensure the correct metadata and attributes
are recorded with the data, and that the data is `CF compliant <https://cfconventions.org/>`__. They are called by the ``ObsSurface`` class.

:func:`~openghg.standardise.surface.parse_aqmesh`
    For processing data from the AQMesh network
:func:`~openghg.standardise.surface.parse_beaco2n`
    For processing data from the BEACO2N network
:func:`~openghg.standardise.surface.parse_btt`
    For processing data from the BT Tower site
:func:`~openghg.standardise.surface.parse_cranfield`
    For processing data from Cranfield
:func:`~openghg.standardise.surface.parse_crds`
    For processing data from CRDS (cavity ring-down spectroscopy) data from the DECC network.
:func:`~openghg.standardise.surface.parse_eurocom`
    For processing data from the EUROCOM network
:func:`~openghg.standardise.surface.parse_gcwerks`
    For processing data in the form expected by the GCWERKS package
:func:`~openghg.standardise.surface.parse_noaa`
    For processing data from the NOAA network
:func:`~openghg.standardise.surface.parse_npl`
    For processing data from NPL
:func:`~openghg.standardise.surface.parse_tmb`
    For processing data from the Thames Barrier site

Metadata handling
^^^^^^^^^^^^^^^^^

These handle the assignment and standardisation of meta`data`.

Attributes
**********

Ensuring the NetCDF created during standardisation has the correct attributes assigned to it.

:func:`~openghg.standardise.meta.assign_attributes`
    Assign attributes to a number of datasets.

:func:`~openghg.standardise.meta.get_attributes`
    Assign attributes to a single dataset, called by the above.

Metadata sync
*************

:func:`~openghg.standardise.meta.sync_surface_metadata`
    Ensure the required metadata is shared between the metadata and attributes.

Storage
=======

These functions and classes handle the lower level storage and retrieval of data from the object store.

Base class
^^^^^^^^^^

This provides the functionality required by all data storage and processing classes, namely the saving, retrieval and loading
of data from the object store.

:class:`~openghg.store.base.BaseStore`
    Base class which the other core processing modules inherit

Datasource
^^^^^^^^^^

The Datasource is the smallest data provider within the OpenGHG topology. A Datasource represents a data provider such as an instrument
measuring a specific gas at a specific height at a specific site. For an instrument measuring three gas species at an inlet height of 100m
at a site we would have three Datasources.

:class:`~openghg.store.base.Datasource`
    Handles the storage of data, metadata and version information for measurements

Emissions
^^^^^^^^^

Handles the storage of emissions data.
:class:`~openghg.dataobjects.Emissions`

EulerianModel
^^^^^^^^^^^^^

Handles the storage of Eulerian model data.

:class:`~openghg.store.EulerianModel`

METStore
^^^^^^^^^

Handles the storage of met data from the ECMWF data.

:class:`~openghg.store.METStore`

Footprints
^^^^^^^^^^

Handles the storage of footprints / flux data.

:class:`~openghg.store.Footprints`

ObsSurface
^^^^^^^^^^

Handles the storage of surface measurement data.

:class:`~openghg.store.ObsSurface`

Dataclasses
===========

These dataclasses are used to facilitate the simple packaging and retrieval of data from the object store.

:class:`~openghg.dataobjects._BaseData`
    The base class that (most of) the dataclasses inherit.

:class:`~openghg.dataobjects.FluxData`
    Stores flux data.

:class:`~openghg.dataobjects.FootprintData`
    Stores footprint data.

:class:`~openghg.dataobjects.METData`
    Stores meteorological data retrieved from the ECMWF / our local cache.

:class:`~openghg.dataobjects.ObsData`
    Stores data returned by search functions.

:class:`~openghg.dataobjects.RankSources`
    Handles the ranking of datasources.

:class:`~openghg.dataobjects.SearchResults`
    Makes the retrieval of data simple.


Retrieval functions
===================

These handle the retrieval of data from the object store.

Searching
^^^^^^^^^

:func:`~openghg.retrieve.search`
    Search for data in the object store, accepts any pair of keyword - argument pairs

    Example usage:

        search(site="bsd", inlet="50m", species="co2", interesting_metadata="special_units")

Specific retrieval
^^^^^^^^^^^^^^^^^^

Handle the retrieval of specific data types, some functions may try to mirror the interface of functions in the Bristol ACRG
repository but should hopefully be useful to all users.

:func:`~openghg.retrieve.get_obs_surface`
    Get measurements from one site

:func:`~openghg.retrieve.get_flux`
    Reads in all flux files for the domain and species as an ``xarray`` Dataset

:func:`~openghg.retrieve.get_footprint`
    Gets footprints from one site


Object Store
============

These functions handle the storage of data in the object store, in JSON or binary format. Each object and piece of data in the
object store is stored at a specific ``key``, which can be thought of as the address of the data. The data is stored
in a ``bucket`` which in the cloud is a section of the OpenGHG object store. Locally a ``bucket`` is just a normal
directory in the user's filesystem specific by the ``OPENGHG_PATH`` environment variable.

:func:`~openghg.objectstore.delete_object`
    Delete an object in the store

:func:`~openghg.objectstore.exists`
    Check if an object exists at that key

:func:`~openghg.objectstore.get_abs_filepaths`
    Get absolute filepaths for objects

:func:`~openghg.objectstore.get_bucket`
    Get path to bucket

:func:`~openghg.objectstore.get_md5`
    Get MD5 has of file

:func:`~openghg.objectstore.get_md5_bytes`
    Get MD5 hash of passed bytes

:func:`~openghg.objectstore.get_object`
    Retrieve object from object store

:func:`~openghg.objectstore.get_object_from_json`
    Retrieve JSON object from object store

:func:`~openghg.objectstore.hash_files`
    Get the MD5 hashes of the given files

:func:`~openghg.objectstore.set_object_from_file`
    Set an object in the object store

:func:`~openghg.objectstore.set_object_from_json`
    Create a JSON object in the object store



Utility functions
=================

This module contains all the helper functions used throughout OpenGHG.

Exporting
^^^^^^^^^

These are used to export data to a format readable by the `OpenGHG data dashboard <https://github.com/openghg/dashboard>`_.

:func:`~openghg.util.to_dashboard`
    Export timeseries data to JSON

:func:`~openghg.util.to_dashboard_mobile`
    Export mobile observations data to JSON

Hashing
^^^^^^^

These handle hashing of data (usually with SHA1)

:func:`~openghg.util.hash_file`
    Calculate the SHA1 hash of a file

:func:`~openghg.util.hash_string`
    Calculate the SHA1 hash of a UTF-8 encoded string


String manipulation
^^^^^^^^^^^^^^^^^^^

String cleaning and formatting functions

:func:`~openghg.util.clean_string`
    Return a lowercase cleaned string

:func:`~openghg.util.to_lowercase`
    Converts a string to lowercase

:func:`~openghg.util.remove_punctuation`
    Removes punctuation from a string

Time
^^^^

Helpers to deal with all things datetime.

:func:`~openghg.util.timestamp_tzaware`
    Create a Timestamp with a UTC timezone

:func:`~openghg.util.timestamp_now`
    Create a timezone aware timestamp for now

:func:`~openghg.util.timestamp_epoch`
    Create a timezone aware timestamp for the UNIX epoch (1970-01-01)

:func:`~openghg.util.daterange_from_str`
    Create a daterange from two timestamp strings

:func:`~openghg.util.daterange_to_str`
    Convert a daterange to string

:func:`~openghg.util.create_daterange_str`
    Create a daterange string from two timestamps or strings

:func:`~openghg.util.create_daterange`
    Create a pandas DatetimeIndex from two timestamps

:func:`~openghg.util.daterange_overlap`
    Check if two dateranges overlap

:func:`~openghg.util.combine_dateranges`
    Combine a list of dateranges

:func:`~openghg.util.split_daterange_str`
    Split a daterange string to the component start and end Timestamps

:func:`~openghg.util.closest_daterange`
    Finds the closest daterange in a list of dateranges

:func:`~openghg.util.valid_daterange`
    Check if the passed daterange is valid

:func:`~openghg.util.find_daterange_gaps`
    Find the gaps in a list of dateranges

:func:`~openghg.util.trim_daterange`
    Removes overlapping dates from to_trim

:func:`~openghg.util.split_encompassed_daterange`
    Checks if one of the passed dateranges contains the other, if so, then
    split the larger daterange into three sections.

:func:`~openghg.util.daterange_contains`
    Checks if one daterange contains another

:func:`~openghg.util.sanitise_daterange`
    Make sure the daterange is correct and return tzaware daterange.

:func:`~openghg.util.check_nan`
    Check if the given value is NaN, is so return an NA string

:func:`~openghg.util.check_date`
    Check if the passed string is a valid date or not, if not returns NA


Iteration
^^^^^^^^^

Our own personal `itertools`

:func:`~openghg.util.pairwise`
    Return a zip of an iterable where a is the iterable
    and b is the iterable advanced one step.

:func:`~openghg.util.unanimous`
    Checks that all values in an iterable object are the same


Site Checks
^^^^^^^^^^^

These perform checks to ensure data processed for each site is correct

:func:`~openghg.util.verify_site`
    Verify that the given site is one we recognize, does fuzzy text matching to suggest a possible valid value.

:func:`~openghg.util.multiple_inlets`
    Check if the passed site has more than one inlet


Cloud
^^^^^

These handle cloud based functionality

:func:`~openghg.util.running_in_cloud`
    Checks if we're running in the cloud by checking for the ``OPENGHG_CLOUD`` environment variable.


Custom Data Types
=================

Errors
^^^^^^

Customised errors for OpenGHG.

:class:`~openghg.util.InvalidSiteError`
    Raised if an invalid site is given

:class:`~openghg.util.UnknownDataError`
    Raised if we don't recognize the data passed

:class:`~openghg.util.FunctionError`
    Raised if there has been an error with a serverless function.

:class:`~openghg.util.ObjectStoreError`
    Raised if an error accessing an object at a key in the object store occurs


Types
^^^^^

These are used in conjunction with ``mypy`` to make type hinting easier.

:class:`~openghg.util.pathType`

:class:`~openghg.util.multiPathType`

:class:`~openghg.util.resultsType`
