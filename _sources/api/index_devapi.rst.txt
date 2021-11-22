=============
Developer API
=============

The functions and methods documented in this section are the internal workings of the OpenGHG library. They are subject to change
without warning due to the early stages of development of the project.

.. warning:: Normal users should not use any of the functions shown here directly as they may be removed or their functionality may change.

modules
=======

Base class
^^^^^^^^^^

This provides the functionality required by all data storage and processing classes, namely the saving, retrieval and loading
of data from the object store.

:class:`~openghg.modules.BaseModule`
    Base class which the other core processing modules inherit

Data processing
^^^^^^^^^^^^^^^

These classes are used for the processing of data by the ``ObsSurface`` processing class. 

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

Datasource
^^^^^^^^^^

The Datasource is the smallest data provider within the OpenGHG topology. A Datasource represents a data provider such as an instrument
measuring a specific gas at a specific height at a specific site. For an instrument measuring three gas species at an inlet height of 100m
at a site we would have three Datasources.

:class:`~openghg.modules.Datasource`
    Handles the storage of data, metadata and version information for measurements


objectstore
============

These functions handle the storage of data in the object store, in JSON or binary format. Each object and piece of data in the
object store is stored at a specific ``key``, which can be thought of as the address of the data. The data is stored
in a ``bucket`` which in the cloud is a section of the OpenGHG object store. Locally a ``bucket`` is just a normal
directory in the user's filesystem specific by the ``OPENGHG_PATH`` environment variable.

:func:`~openghg.objectstore.delete_object`
    Delete an object in the store
:func:`~openghg.objectstore.exists`
    Check if an object exists at that key
:func:`~openghg.objectstore.get_bucket`
    Get path to bucket
:func:`~openghg.objectstore.get_local_bucket`
    Get path to local bucket
:func:`~openghg.objectstore.get_object`
    Get object at given key
:func:`~openghg.objectstore.get_object_from_json`
    Get object from JSON
:func:`~openghg.objectstore.set_object_from_file`
    Set data at a key from a given filepath
:func:`~openghg.objectstore.set_object_from_json`
    Set data at a key from JSON

util
====

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

:func:`~openghg.util.valid_site`
    Check if the passed site is a valid one

:func:`~openghg.util.multiple_inlets`
    Check if the passed site has more than one inlet
