====
Util
====

Helper functions that are used throughout OpenGHG. From file hashing to timestamp handling.

Domain
======

.. autofunction:: openghg.util.convert_longitude

.. autofunction:: openghg.util.find_domain


Downloading data
================

.. autofunction:: openghg.util.download_data

.. autofunction:: openghg.util.parse_url_filename

File handling, compression
==========================

.. autofunction:: openghg.util.compress

.. autofunction:: openghg.util.compress_json

.. autofunction:: openghg.util.compress_str

.. autofunction:: openghg.util.decompress

.. autofunction:: openghg.util.decompress_json

.. autofunction:: openghg.util.decompress_str

.. autofunction:: openghg.util.get_datapath

.. autofunction:: openghg.util.get_logfile_path

.. autofunction:: openghg.util.load_column_parser

.. autofunction:: openghg.util.load_column_source_parser

.. autofunction:: openghg.util.load_emissions_database_parser

.. autofunction:: openghg.util.load_emissions_parser

.. autofunction:: openghg.util.load_json

.. autofunction:: openghg.util.load_surface_parser

.. autofunction:: openghg.util.read_header

Hashing
=======

.. autofunction:: openghg.util.hash_bytes

.. autofunction:: openghg.util.hash_file

.. autofunction:: openghg.util.hash_retrieved_data

.. autofunction:: openghg.util.hash_string


Measurement helpers
===================

.. autofunction:: openghg.util.check_lifetime_monthly

.. autofunction:: openghg.util.format_inlet

.. autofunction:: openghg.util.find_matching_site

.. autofunction:: openghg.util.multiple_inlets

.. autofunction:: openghg.util.molar_mass

.. autofunction:: openghg.util.species_lifetime

.. autofunction:: openghg.util.synonyms

.. autofunction:: openghg.util.site_code_finder

.. autofunction:: openghg.util.verify_site

String handling
===============

.. autofunction:: openghg.util.clean_string

.. autofunction:: openghg.util.is_number

.. autofunction:: openghg.util.remove_punctuation

.. autofunction:: openghg.util.to_lowercase

Dates and times
===============

.. autofunction:: openghg.util.check_date

.. autofunction:: openghg.util.check_nan

.. autofunction:: openghg.util.closest_daterange

.. autofunction:: openghg.util.combine_dateranges

.. autofunction:: openghg.util.create_daterange

.. autofunction:: openghg.util.create_daterange_str

.. autofunction:: openghg.util.create_frequency_str

.. autofunction:: openghg.util.daterange_contains

.. autofunction:: openghg.util.daterange_from_str

.. autofunction:: openghg.util.daterange_overlap

.. autofunction:: openghg.util.daterange_to_str

.. autofunction:: openghg.util.find_daterange_gaps

.. autofunction:: openghg.util.find_duplicate_timestamps

.. autofunction:: openghg.util.first_last_dates

.. autofunction:: openghg.util.in_daterange

.. autofunction:: openghg.util.parse_period

.. autofunction:: openghg.util.relative_time_offset

.. autofunction:: openghg.util.sanitise_daterange

.. autofunction:: openghg.util.split_daterange_str

.. autofunction:: openghg.util.split_encompassed_daterange

.. autofunction:: openghg.util.time_offset

.. autofunction:: openghg.util.time_offset_definition

.. autofunction:: openghg.util.timestamp_epoch

.. autofunction:: openghg.util.timestamp_now

.. autofunction:: openghg.util.timestamp_tzaware

.. autofunction:: openghg.util.trim_daterange

.. autofunction:: openghg.util.valid_daterange

User
====

Handling user configuration files.

.. autofunction:: openghg.util.create_default_config

.. autofunction:: openghg.util.get_user_config_path

.. autofunction:: openghg.util.read_local_config

Environment detection
=====================

.. autofunction:: openghg.util.running_locally

.. autofunction:: openghg.util.running_in_cloud

.. autofunction:: openghg.util.running_on_hub

Miscellaneous
=============

Some `itertools` like functions.

.. autofunction:: openghg.util.pairwise

.. autofunction:: openghg.util.unanimous
