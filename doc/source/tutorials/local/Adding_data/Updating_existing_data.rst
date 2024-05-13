.. _updating_existing_data:
Updating existing data
======================

OpenGHG categorises data based on the supplied necessary keywords and stores these as the associated metadata. For each data type these keywords will be different but they will always be used to understand how the data is defined.

When adding data to the object store, two checks will be made against currently stored data:

1. Whether data has the same set of distinct keywords.
2. Whether the time range for the data being added overlaps with the current time range for that data.

If the data exists but the time range does not overlap, this data will be added, grouped with the previous data and associated with the same keywords.

By default, if data exists and the time range *does* overlap with existing data, the data will not be added and this will produce a ``DataOverlapError``.

Updating data
-------------

To add updated data to the object store which does overlap on time with current data, when using the ``standarise_*`` functions the user can specify what action to perform in this case using the ``if_exists`` input. This provides the options:

1. "auto" - combine with previous data if no overlapping data points, raise ``DataOverlap`` error otherwise (default).
2. "new" - store the newly added data (only)
3. *"combine" - combine the new and previous data and prefer the new data in the case where the time range overlaps. - to be implemented.*

By default, using the "new" option will also create a new version of the data. In this way, the previous data will be retained (saved) but the new data will become the details which are accessed by default.

Managing versions
-----------------

If data files are large or there will be many updates needed, it may not be desirable to save the currently stored data and it may be prefered to delete this rather than retain this as a version. Whether to retain or overwrite the current data can set using the `save_current` input.

1. "auto"

    a. if data does not overlap, retain current data and version. 
    b. if data does overlap and ``if_exists="auto"``, raise ``DataOverlap`` error.
    c. if data does overlap and ``if_exists="new"``, save current data and create a new version.

2. "yes" (/"y") - Save the current data and create a new version for the new data.
3. "no" (/"n") - Do not save the current data and replace with the new data.


Replacing "identical" data
--------------------------

One check OpenGHG will make will be whether or not an exact copy of this file has been added previously. In this case this will not check within the object store explicitly, and the data will not be added. For the rare cases where this may not be the desired behaviour, the `force` flag using `True` or `False`can be used to bypass this check and attempt to add the data to the object store in the usual way. 

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
This data includes may different species so we will focus on just $CF_6$ for this tutorial.

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
not overlap with the data in our datasource.

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

Examining the metadata we should not see this only includes the new data from 2012 and latest_version has increased by 1.

4. Replacing existing data with new data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If we wanted to update the data but did *not* want to retain the current latest version
of the data we can do this using the flags:

* ``if_exists="new"``
* ``save_current=False``

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
                        save_current=False)

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

5. Replacing "identical" data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There may be circumstances (e.g. data corruption, testing) where it is necessary to
replace "identical" data (i.e. the same original input file with the same details).

This can be done using the flag:

* ``force=True``

.. code:: ipython3

    standardise_surface(filepaths=data_2013,
                        source_format=source_format,
                        site=site,
                        network=network,
                        force=True)

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

By default this will create a new version as shown above.

To avoid this pass both the ``force`` and ``save_current`` flag instead:

* ``force=True``
* ``save_current=False``

.. code:: ipython3

    standardise_surface(filepaths=data_2013,
                        source_format=source_format,
                        site=site,
                        network=network,
                        force=True,
                        save_current=False)

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
