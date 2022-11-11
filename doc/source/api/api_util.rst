====
Util
====

Exporting
=========

These are used to export data to a format readable by the `OpenGHG data dashboard <https://github.com/openghg/dashboard>`_.

.. autofunction:: openghg.util.to_dashboard

.. autofunction:: openghg.util.to_dashboard_mobile

Hashing
=======

These handle hashing of data (usually with SHA1)

.. autofunction:: openghg.util.hash_file

.. autofunction:: openghg.util.hash_string


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
