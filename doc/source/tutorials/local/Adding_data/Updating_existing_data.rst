.. _updating_existing_data:
Updating existing data
======================

OpenGHG categorises data based on the supplied necessary keywords and stores these as the associated metadata. For each data type these keywords will be different but they will always be used to understand how the data is defined.

When adding data to the object store, two checks will be made against currently stored data:

1. Whether data has the same set of distinct keywords.
2. Whether any time-coordinate values in the data being added match values
   already stored for that data.

By default, this is an exact check for matching time-coordinate values rather
than a check that the start and end date ranges intersect.

If the data exists but there are no matching time-coordinate values, the new data
will be added, grouped with the previous data and associated with the same
keywords when using the default ``if_exists="auto"`` policy.

By default, if data exists and matching time-coordinate values are found, the
data will not be added and this will produce a ``DataOverlapError``.

Updating data
-------------

To add updated data to the object store, when using the ``standardise_*``
functions the user can specify what action to perform using the ``if_exists``
input. This provides the options:

1. ``"auto"`` - add to the current data if there are no matching
   time-coordinate values; raise ``DataOverlapError`` otherwise (default).
2. ``"new"`` - make the latest version contain only the newly added data.
3. ``"combine"`` - combine the new and previous data and prefer the new data where
   time-coordinate values match.

``if_exists`` determines the contents of the resulting latest version;
``save_current`` determines whether that result is written to a new version or
into the current latest version.

These choices are deliberate versioning behaviour, not a special case of how
files are passed in. When a list of filepaths is successfully concatenated by
the parser, the ``if_exists`` policy is applied once to the combined dataset.
Calling the same function repeatedly in a Python loop applies the policy once
per call. Both approaches use the same policy, but the resulting version history
can differ depending on how the parser groups the inputs. For a growing time
series, use ``if_exists="auto"`` for files without matching time-coordinate
values or ``if_exists="combine"`` when new files should be merged with current
data.

Managing versions
-----------------

If data files are large or there will be many updates needed, it may not be
desirable to save the currently stored data and it may be preferred to delete
this rather than retain it as a version. Whether to retain or overwrite the
current data can be set using the ``save_current`` input.

When a matching datasource already exists, the update behaviour is:

.. list-table::
   :header-rows: 1

   * - ``if_exists``
     - ``save_current="auto"``
     - ``save_current="y"``
     - ``save_current="n"``
   * - ``"auto"``
     - Append points with new time-coordinate values in place.
     - Copy current data to a new version, then append points with new
       time-coordinate values.
     - Append points with new time-coordinate values in place.
   * - ``"new"``
     - Create a new version containing only new data.
     - Create a new version containing only new data.
     - Replace the current latest version with new data.
   * - ``"combine"``
     - Create a new version containing old and new data, preferring new data at
       matching times.
     - Create a new version containing old and new data, preferring new data at
       matching times.
     - Update the current latest version in place, preferring new data at
       matching times.

For every ``if_exists="auto"`` case, matching time-coordinate values still raise
``DataOverlapError``. Repeated calls with ``if_exists="combine"`` and
``save_current="auto"`` create a new version on each update after the first; use
``save_current="n"`` to combine in place.


Repeating input data
--------------------

OpenGHG does not identify repeated input files separately from their data. Repeating an
input therefore follows the same ``if_exists`` update policy as any other update. With
the default ``if_exists="auto"``, matching time-coordinate values raise a
``DataOverlapError``. Use ``if_exists="new"`` to replace the latest data with
the repeated input, or ``if_exists="combine"`` to combine it with the current
data.

Example workflow
----------------

This section includes an example workflow of how these keywords can be used.
The sections must be completed in order to produce the expected results.

0. Using the tutorial object store
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For these sections, you should work in a sandboxed object
store called the tutorial store to produce the expected results.
To do this we use the
``use_tutorial_store`` function from ``openghg.tutorial``. This sets the
``OPENGHG_TUT_STORE`` environment variable for this session and won't
affect your use of OpenGHG outside of this tutorial.

.. code:: ipython3

    from openghg.tutorial import use_tutorial_store

    use_tutorial_store()

Since this workflow relies on using an empty object store, we also recommend
running ``clear_tutorial_store`` as well before using this in case other tutorials
have been run using this store.

.. code:: ipython3

    from openghg.tutorial import clear_tutorial_store

    clear_tutorial_store()

1. Adding example data
^^^^^^^^^^^^^^^^^^^^^^

We can grab some example data to demonstrate this workflow, in this case from the Macehead site in Ireland.
This data includes many different species so we will focus on just ``CF_4`` for this tutorial.

.. code:: ipython3

    from openghg.tutorial import retrieve_example_data

    data_url = "https://github.com/openghg/example_data/raw/main/timeseries/mhd_2010.tar.gz"

    data_2010 = retrieve_example_data(url=data_url)
    data_2010 = (data_2010[0], data_2010[1])  # for this specific data need to reorganise to include file and precision data.

After retrieving the data we can set up our keywords and add data to tutorial store.

.. code:: ipython3

    source_format="GCWERKS"
    site="MHD"
    network="AGAGE"

.. code:: ipython3

    from openghg.standardise import standardise_surface

    standardise_surface(filepaths=data_2010,
                        source_format=source_format,
                        site=site,
                        network=network)


If we search the tutorial object store we should now see one datasource has been returned.

.. code:: ipython3

    from openghg.retrieve import search_surface

    data_search = search_surface(site=site, species="cf4")
    results = data_search.results
    results

We can also examine the metadata for this datasource we can see what details have been stored:

.. code:: ipython3

    data_search.retrieve().metadata

Selected output:

.. code:: ipython3

    {
    ...
    'start_date': '2010-01-01 02:10:00+00:00',
    'end_date': '2010-12-31 20:53:59+00:00',
    'latest_version': 'v1',
    ...
    }

This shows the ``start_date``, ``end_date`` and ``latest_version`` of the data stored within the object store.
The start and end dates cover the year of 2010: 2010-01-01 - 2010-12-31.

2. Adding more data
^^^^^^^^^^^^^^^^^^^

We can now download and add data for the next year (2011). The times for this data should
not match any time-coordinate values in our datasource.

.. code:: ipython3

    from openghg.tutorial import retrieve_example_data

    data_url = "https://github.com/openghg/example_data/raw/main/timeseries/mhd_2011.tar.gz"

    data_2011 = retrieve_example_data(url=data_url)
    data_2011 = (data_2011[0], data_2011[1])  # for this specific data need to reorganise to include file and precision data.

.. code:: ipython3

    from openghg.standardise import standardise_surface

    standardise_surface(filepaths=data_2011,
                        source_format=source_format,
                        site=site,
                        network=network)

When we search we should see there is still only one entry returned.

.. code:: ipython3

    from openghg.retrieve import search_surface

    data_search2 = search_surface(site=site, species="cf4")
    results = data_search2.results
    results

.. code:: ipython3

    data_search2.retrieve().metadata

Selected output:

.. code:: ipython3

    {
    ...
    'start_date': '2010-01-01 02:10:00+00:00',
    'end_date': '2011-12-31 22:30:59+00:00',
    'latest_version': 'v1',
    ...
    }

By examining the metadata we can see that the start and end dates now extend from 2010 the end of 2011: 2010-01-01 to 2011-12-31 and the latest_version is still the same.
This has combined the details from both files that were added to the object store into one datasource.

3. Updating with new data
^^^^^^^^^^^^^^^^^^^^^^^^^

If we wanted to use the same flags but add new data only, we can do this using the input flag:

* ``if_exists="new"``

By default this will also create a new version, retaining the original data as a previous
version but ensuring that the new data will returned when searching. In this case we have downloaded
data from 2012 to check this.

.. code:: ipython3

    from openghg.tutorial import retrieve_example_data

    data_url = "https://github.com/openghg/example_data/raw/main/timeseries/mhd_2012.tar.gz"

    data_2012 = retrieve_example_data(url=data_url)
    data_2012 = (data_2012[0], data_2012[1])  # for this specific data need to reorganise to include file and precision data.

.. code:: ipython3

    from openghg.standardise import standardise_surface

    standardise_surface(filepaths=data_2012,
                        source_format=source_format,
                        site=site,
                        network=network,
                        if_exists="new")

We should still only see one datasource when we search:

.. code:: ipython3

    from openghg.retrieve import search_surface

    data_search3 = search_surface(site=site, species="cf4")
    data_search3.results

.. code:: ipython3

    data_search3.retrieve().metadata

Selected output:

.. code:: ipython3


    {
    ...
    'start_date': '2012-01-01 02:11:00+00:00',
    'end_date': '2012-12-31 12:38:59+00:00',
    'latest_version': 'v2',
    ...
    }

Examining the metadata we should now see this includes only the new data from 2012 and latest_version has increased by 1.

4. Replacing existing data with new data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If we wanted to update the data but did *not* want to retain the current latest version
of the data we can do this using the flags:

* ``if_exists="new"``
* ``save_current="n"``

We can test this by downloading data for the same site from 2013.

.. code:: ipython3

    from openghg.tutorial import retrieve_example_data

    data_url = "https://github.com/openghg/example_data/raw/main/timeseries/mhd_2013.tar.gz"

    data_2013 = retrieve_example_data(url=data_url)
    data_2013 = (data_2013[0], data_2013[1])  # for this specific data need to reorganise to include file and precision data.

.. code:: ipython3

    from openghg.standardise import standardise_surface

    standardise_surface(filepaths=data_2013,
                        source_format=source_format,
                        site=site,
                        network=network,
                        if_exists="new",
                        save_current="n")

Searching should return one datasource as before:

.. code:: ipython3

    from openghg.retrieve import search_surface

    data_search4 = search_surface(site=site, species="cf4")
    data_search4.results

.. code:: ipython3

    data_search4.retrieve().metadata

Selected output:

.. code:: ipython3

    {
    ...
    'start_date': '2013-01-01 02:19:00+00:00',
    'end_date': '2013-12-29 16:14:59+00:00',
    'latest_version': 'v2',
    ...
    }

This now contains new data only from 2013 but the version has not changed
(indicating the previous version data has not been retained).

5. Repeating input data
^^^^^^^^^^^^^^^^^^^^^^^

There may be circumstances (e.g. data corruption or testing) where it is necessary to
replace data using the same original input file. Choose the update explicitly with:

* ``if_exists="new"``

.. code:: ipython3

    standardise_surface(filepaths=data_2013,
                        source_format=source_format,
                        site=site,
                        network=network,
                        if_exists="new")

.. code:: ipython3

    data_search5 = search_surface(site=site, species="cf4")
    data_search5.retrieve().metadata

Selected output:

.. code:: ipython3

    {
    ...
    'start_date': '2013-01-01 02:19:00+00:00',
    'end_date': '2013-12-29 16:14:59+00:00',
    'latest_version': 'v3',
    ...
    }

By default, ``if_exists="new"`` creates a new version as shown above.

To replace the latest version without retaining the current version, also pass
``save_current="n"``:

* ``if_exists="new"``
* ``save_current="n"``

.. code:: ipython3

    standardise_surface(filepaths=data_2013,
                        source_format=source_format,
                        site=site,
                        network=network,
                        if_exists="new",
                        save_current="n")

.. code:: ipython3

    data_search6 = search_surface(site=site, species="cf4")
    data_search6.retrieve().metadata

Selected output:

.. code:: ipython3

    {
    ...
    'start_date': '2013-01-01 02:19:00+00:00',
    'end_date': '2013-12-29 16:14:59+00:00',
    'latest_version': 'v3',
    ...
    }

This should include the same start, end date and latest_version as the previous search output.

6. Cleanup
^^^^^^^^^^

If you're finished with the data in this tutorial you can cleanup the
tutorial object store using the ``clear_tutorial_store`` function again.

.. code:: ipython3

    from openghg.tutorial import clear_tutorial_store

.. code:: ipython3

    clear_tutorial_store()
