====
Util
====

Exporting
=========

These are used to export data to a format readable by the `OpenGHG data dashboard <https://github.com/openghg/dashboard>`_.

.. autofunction:: openghg.util.to_dashboard

.. autofunction:: openghg.util.to_dashboard_mobile

Xarray metadata and history
===========================

Use ``with_xarray_metadata`` to attach a dataclass or mapping as one JSON string
attribute without changing the input ``DataArray`` or ``Dataset``. The JSON has
``schema_version: 1`` and a ``metadata`` mapping. Values may be strings, booleans,
finite numbers, nulls, lists, and nested string-keyed mappings. Other values,
including NumPy scalars, dates, tuples, and non-finite numbers, must be converted
by the caller. ``decode_xarray_metadata`` rejects malformed JSON, duplicate keys,
missing or unsupported schema versions, and invalid values. Domain-specific metadata fields
remain the caller's responsibility.

``append_xarray_history`` adds a UTC timestamped line to the CF ``history``
attribute. It preserves existing history and other attributes, returns the same
xarray type, and leaves the input unchanged.

.. autofunction:: openghg.util.encode_xarray_metadata

.. autofunction:: openghg.util.decode_xarray_metadata

.. autofunction:: openghg.util.with_xarray_metadata

.. autofunction:: openghg.util.append_xarray_history

String manipulation
===================

String cleaning and formatting functions

.. autofunction:: openghg.util.clean_string

.. autofunction:: openghg.util.to_lowercase

.. autofunction:: openghg.util.remove_punctuation

Time
====

Helpers to deal with all things datetime.

.. autofunction:: openghg.util.timestamp_tzaware

.. autofunction:: openghg.util.timestamp_now

.. autofunction:: openghg.util.timestamp_epoch

.. autofunction:: openghg.util.daterange_from_str

.. autofunction:: openghg.util.daterange_to_str

.. autofunction:: openghg.util.create_daterange_str

.. autofunction:: openghg.util.create_daterange

.. autofunction:: openghg.util.check_nan

.. autofunction:: openghg.util.check_date


Site Checks
===========

These perform checks to ensure data processed for each site is correct

.. autofunction:: openghg.util.verify_site

.. autofunction:: openghg.util.multiple_inlets


Domain
======

.. autofunction:: openghg.util.find_domain

.. autofunction:: openghg.util.convert_longitude

Inlet
=====

.. autofunction:: openghg.util.format_inlet
