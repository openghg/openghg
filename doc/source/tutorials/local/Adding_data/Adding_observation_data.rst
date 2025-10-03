.. _adding-obs-data:
Adding observation data
=======================

.. jupyter-execute::
    :hide-code:

    import logging
    import openghg

    logger = logging.getLogger("openghg")
    for handler in logger.handlers:
        handler.setLevel(logging.ERROR)


This tutorial demonstrates how OpenGHG can be used to process new
measurement data, search the data present and to retrieve this for
analysis and visualisation.

.. _what-is-object-store:

What is an object store?
------------------------

Each object and piece of data in the object store is stored at a specific key, which can be thought of as the address of the data. The data is stored in a bucket which in the cloud is a section of the OpenGHG object store. Locally a bucket is just a normal directory in the user’s filesystem specified by the path given in the configuration file at ``~/.config/openghg/openghg.conf``.


.. _using-the-tutorial-object-store:

0. Using the tutorial object store
----------------------------------

An object store is a folder with a fixed structure within which openghg
can read and write data. To avoid adding the example data we use in this
tutorial to your normal object store, we need to tell OpenGHG to use a
separate sandboxed object store that we'll call the tutorial store. To do
this we use the ``use_tutorial_store`` function from ``openghg.tutorial``.
This sets the ``OPENGHG_TUT_STORE`` environment variable for this session and
won't affect your use of OpenGHG outside of this tutorial.

.. jupyter-execute::

    from openghg.tutorial import use_tutorial_store

    use_tutorial_store()

1. Adding and standardising data
--------------------------------

.. note::
    Outside of this tutorial, if you have write access to multiple object stores you
    will need to pass the name of the object store you wish to write to to
    the ``store`` argument of the standardise functions.

Source formats
~~~~~~~~~~~~~~

OpenGHG can process and store several source formats in the object store,
including data from the AGAGE, DECC, NOAA, LondonGHG, BEAC2ON networks.
The process of adding data to the object store is called *standardisation*.

To standardise a new data file, you must specify the *source format* and
other keywords for the data. Which keywords need to be specified may be dependent
on the source format itself as some details can be inferred from the data or may
not be relevant.
For the full list of accepted observation inputs and source formats, call
the function ``summary_source_formats``:

.. jupyter-execute::

    from openghg.standardise import summary_source_formats

    summary = summary_source_formats()

    ## UNCOMMENT THIS CODE TO SHOW ALL ENTRIES
    # import pandas as pd; pd.set_option('display.max_rows', None)

    summary

There may be multiple source formats for a given site.
For instance, the Tacolneston site in the UK (site code “TAC”) has four entries:

.. jupyter-execute::

    summary[summary["Site code"] == "TAC"]


Let's see what data is available for a given source.
First, we'll list all source formats.

.. jupyter-execute::

    summary["Source format"].unique()

Now we'll find all data with source format ``"CRDS"``.

.. jupyter-execute::

    summary[summary["Source format"] == "CRDS"]

DECC network
~~~~~~~~~~~~

We will start by adding data to the object store from Tacolneston, which is a *surface site*
in the DECC network. (Data at surface sites is measured in-situ.)

First we retrieve the raw data.

.. jupyter-execute::

    from openghg.tutorial import retrieve_example_data

    data_url = "https://github.com/openghg/example_data/raw/main/timeseries/tac_example.tar.gz"

    tac_data = retrieve_example_data(url=data_url)


Now we add this data to the object store using ``standardise_surface``, passing the
following arguments:

* ``filepath``: list of paths to ``.dat`` files
* ``site``:  ``"TAC"``, the site code for Tacolneston
* ``network``: ``"DECC"``
* ``source_format``: ``"CRDS"``, the type of data we want to process

.. jupyter-execute::

    from openghg.standardise import standardise_surface

    decc_results = standardise_surface(filepath=tac_data, source_format="CRDS", site="TAC", network="DECC")

    decc_results

This extracts the data and metadata from the files,
standardises them, and adds them to our object store. The keywords of ``site`` and ``network``,
along with details extracted from the data itself allow us to uniquely store the data.

The returned ``decc_results`` dictionary shows how the data
has been stored: each file has been split into several entries, each with a unique ID (UUID).
Each entry is known as a *Datasource* (see :ref:`note-on-datasources`).

The ``decc_results`` output includes details of the processed data and tells
us that the data has been stored correctly. This will also tell us if
any errors have been encountered when trying to access and standardise
this data.

AGAGE data
~~~~~~~~~~

OpenGHG can also process data from the `AGAGE network <https://agage.mit.edu/>`_.

Historically, the AGAGE network produces output files from GCWERKS alongside a seperate *precisions file*. If you wish
to use this form of input file, we create a tuple with the data filename and the precisions filename. For example:

First we retrieve example data from the  Cape Grim station in Australia (site code "CGO"").

.. jupyter-execute::

    cgo_url = "https://github.com/openghg/example_data/raw/main/timeseries/capegrim_example.tar.gz"

    capegrim_data = retrieve_example_data(url=cgo_url)

``capegrim_data`` is a list of two file paths, one for the data file and one for the precisions file:

.. code:: python

    from pathlib import Path

    base_path = Path.home() / "openghg_store" / "tutorial_store" / "extracted_files"
    files = [
        base_path / "capegrim.18.C",
        base_path / "capegrim.18.precisions.C"
    ]


We put the data file and precisions file into a tuple:

.. jupyter-execute::

    capegrim_tuple = (capegrim_data[0], capegrim_data[1])

We can add these files to the object store in the same way as the DECC
data by including the right arguments:

* ``filepath``: tuple (or list of tuples) with paths to data and precision files
* ``site`` (site code): ``"CGO"``
* ``network``: ``"AGAGE"``
* ``instrument``: ``"medusa"``
* ``source_format`` (data type): ``"GCWERKS"``

.. jupyter-execute::

    agage_results = standardise_surface(filepath=capegrim_tuple, source_format="GCWERKS", site="CGO",
                                  network="AGAGE", instrument="medusa")
    agage_results

When viewing ``agage_results`` there will be a large number of
Datasource UUIDs shown due to the large number of gases in each data
file

However, recently the AGAGE network has begun to also produce netCDF files, which are processed by Matt
Rigby's `agage-archive <https://github.com/mrghg/agage-archive>`_ repository. These files are split by site,
species and instrument and do not need an accompanying precisions file. These can also be read in by the
``openghg.standardise.standardise_surface`` function, with the arguments:

* ``filepath``: filepath to the .nc file
* ``site`` (site code): ``"CGO"``
* ``source_format`` (data type): ``"AGAGE"``
* ``network``: ``"AGAGE"``
* ``instrument``: ``"medusa"``

The data will be processed in the same way as the old AGAGE data, and stored in the object store accordingly.
Ensure that the ``source_format`` argument matches the input filetype, as the two are not compatible.

.. _note-on-datasources:

Note on Datasources
^^^^^^^^^^^^^^^^^^^

Datasources are objects that are stored in the `object store <https://docs.openghg.org/api/devapi_objectstore.html>`_ that hold the data and metadata associated with
each measurement we upload to the platform.

For example, if we upload a file that contains readings for three gas
species from a single site at a specific inlet height OpenGHG will
assign this data to three different Datasources, one for each species.
Metadata such as the site, inlet height, species, network etc are stored
alongside the measurements for easy searching.

Datasources can also handle multiple versions of data from a single
site, so if scales or other factors change multiple versions may be
stored for easy future comparison.

Other keywords
~~~~~~~~~~~~~~

When adding data in this way there are other keywords which can be used to
distinguish between different data sets as required including:

* ``instrument``: Name of the instrument
* ``sampling_period``: The time taken for each measurement to be sampled
* ``data_level``: The level of quality control which has been applied to the data.
* ``data_sublevel``:  Optional level to include between data levels. Typically for level 1 data where multiple steps of initial QA may have been applied.
* ``dataset_source``: Name of the dataset if data is taken from a larger source e.g. from an ObsPack

See the `standardise_surface` documentation for a full list of inputs.


Informational keywords
~~~~~~~~~~~~~~~~~~~~~~

In addition to the keywords demonstrated for adding data and described above which are used to distinguish
between different data sets being stored, the following informational details can also be added to help describe the data.

Using the `tag` keyword
^^^^^^^^^^^^^^^^^^^^^^^

The `tag` keyword allows one or multiple short labels to be specified which can be the same across multiple
data sources. For instance, data from different sites which is associated with a particular project could all be
added using the same `tag`. For example below we show how to add the same data as above with a `tag`:

* Tacolneston (TAC) data with a tag of "project1"
* Cape Grim (CGO) data with a tag of both "project1" and "project2"

.. jupyter-execute::

    from openghg.standardise import standardise_surface

    decc_results = standardise_surface(filepath=tac_data,
                                       source_format="CRDS",
                                       site="TAC",
                                       network="DECC",
                                       tag="project1",
                                       force=True)

    agage_results = standardise_surface(filepath=capegrim_tuple,
                                        source_format="GCWERKS",
                                        site="CGO",
                                        network="AGAGE",
                                        instrument="medusa",
                                        tag=["project1", "project2"],
                                        force=True)


*Note: here we included the force=True keyword as we are adding the same data which has been added in
a previous step of the tutorial - see "Updating existing data" tutorial for more details of this.*

As will be covered in the :ref:`2. Searching for data` section, these keywords can then used when searching the
object store. For the `tag` keyword this can be used to return all data which includes the chosen tag.

Adding informational keys
^^^^^^^^^^^^^^^^^^^^^^^^^

Informational keys and associated values can also be added using the `info_metadata` input. The most
common example for this would be to add a `comment` input. For example:

.. code:: ipython3

    decc_results = standardise_surface(filepath=tac_data,
                                       source_format="CRDS",
                                       site="TAC",
                                       network="DECC",
                                       info_metadata={"comment": "Automatic quality checks have been applied."})

Note that for both `info_metadata` and `tag` that these options are available for all data types (not just
observations).

Multiple stores
~~~~~~~~~~~~~~~

If you have write access to more than one object store you'll need to pass in the name of that store
to the ``store`` argument.
So instead of the standardise_surface call above, we'll tell it to write to our default ``user`` object store. This is our default local object store
created when we run ``openghg --quickstart``.

.. code:: ipython3

    from openghg.standardise import standardise_surface

    decc_results = standardise_surface(filepath=tac_data, source_format="CRDS", site="TAC", network="DECC", store="user")

The ``store`` argument can be passed to any of the ``standardise`` functions in OpenGHG and is required if you have write access
to more than one store.

2. Searching for data
---------------------

Searching the object store
~~~~~~~~~~~~~~~~~~~~~~~~~~

We can search the object store by property using the
``search_surface(...)`` function. This function retrieves all of the metadata associated with the search query from the data in the object store.

For example we can find all sites which have measurements for carbon
tetrafluoride (“cf4”) using the ``species`` keyword:

.. jupyter-execute::

    from openghg.retrieve import search_surface

    cfc_results = search_surface(species="cfc11")
    cfc_results.results

We could also look for details of all the data measured at the Tacolneston
(“TAC”) site using the ``site`` keyword:

.. jupyter-execute::

    tac_results = search_surface(site="tac")
    tac_results.results

For this site you can see this contains details of each of the species
as well as the inlet heights these were measured at.

Searching by `tag` keyword
~~~~~~~~~~~~~~~~~~~~~~~~~~

We can also search by the `tag` keyword when this has been set. Even though the `tag`
keyword can contain multiple values, this will find all the datasources where the
tag value is included (rather than needing an exact match like the other keywords).

For the "TAC" and "CGO" data we added the "project1" tag and so this data can be found
using this keyword:

.. jupyter-execute::

    results = search_surface(tag="project1")
    results.results

For the "CGO" data we also included the "project2" tag and we can find this
data by searching for this:

.. jupyter-execute::

    results = search_surface(tag="project2")
    results.results

Quickly retrieve data
~~~~~~~~~~~~~~~~~~~~~

Say we want to retrieve all the ``co2`` data from Tacolneston, we can
perform perform a search and expect a |SearchResults|_
object to be returned. If no results are found ``None`` is returned.

.. |SearchResults| replace:: ``SearchResults``
.. _SearchResults: https://docs.openghg.org/api/api_dataobjects.html#openghg.dataobjects.SearchResult

.. jupyter-execute::

    results = search_surface(site="tac", species="co2")
    results.results

We can retrieve either some or all of the data easily using the
``retrieve`` function.

.. jupyter-execute::

    inlet_54m_data = results.retrieve(inlet="54m")
    inlet_54m_data

Or we can retrieve all of the data and get a list of ``ObsData``
objects.

.. jupyter-execute::

    all_co2_data = results.retrieve_all()
    all_co2_data

3. Retrieving data
------------------

To retrieve the standardised data from the object store there are
several functions we can use which depend on the type of data we want to
access.

To access the surface data we have added so far we can use the
``get_obs_surface`` function and pass keywords for the site code,
species and inlet height to retrieve our data. Using `get_*` functions will only allow one set of data to be returned and will give details if this is not the case.

In this case we want to extract the carbon dioxide (“co2”) data from the
Tacolneston data (“TAC”) site measured at the “185m” inlet:

.. jupyter-execute::

    from openghg.retrieve import get_obs_surface

    co2_data = get_obs_surface(site="tac", species="co2", inlet="185m")

If we view our returned ``obs_data`` variable this will contain:

-  ``data`` - The standardised data (accessed using
   e.g. ``obs_data.data``). This is returned as an `xarray
   Dataset <https://xarray.pydata.org/en/stable/generated/xarray.Dataset.html>`__.
-  ``metadata`` - The associated metadata (accessed using
   e.g. ``obs_data.metadata``).

.. jupyter-execute::

    co2_data

.. jupyter-execute::

    co2_data.data

.. jupyter-execute::

    co2_data.metadata

We can now make a simple plot using the ``plot_timeseries`` method of
the ``ObsData`` object.

   **NOTE:** the plot created below may not show up on the online
   documentation version of this notebook.

.. jupyter-execute::

    co2_data.plot_timeseries()

You can also pass any of ``title``, ``xlabel``, ``ylabel`` and ``units``
to the ``plot_timeseries`` function to modify the labels.

Unit conversion
^^^^^^^^^^^^^^^^

You can request the mole fraction data in a different unit by specifying
the `target_units` argument when calling ``get_obs_surface``.

For example, to convert the mole fraction from the default unit
(usually ppm for CO₂) to ppb:

.. jupyter-execute::

    co2_ppb = get_obs_surface(
        site="tac", species="co2", inlet="185m", target_units={"mf": "ppb"}
    )

.. jupyter-execute::

    co2_ppb.data

By default, the returned data is dequantified, so you can confirm the unit conversion using:

.. jupyter-execute::

    co2_ppb.data["mf"].attrs["units"]

This confirms that the mole fraction (``mf``) was converted to **parts per billion (ppb)** instead of the default **parts per million (ppm)**. The original units attribute is preserved in scalar format compatible with the further workflow.
We can display units in other formats:

.. jupyter-execute::

   # quantify, then get pint units
   pint_units = co2_ppb.data.mf.pint.quantify().pint.units

   # print in cf format
   print(f"{pint_units:cf}")

   # print in default format
   print(f"{pint_units:D}")

If you prefer to keep the data **quantified** (i.e., retaining the Pint unit objects), set the ``is_dequantified`` argument to ``False`` when calling ``get_obs_surface``.

.. jupyter-execute::

    co2_ppb_quantified = get_obs_surface(site="tac", species="co2", inlet="185m", target_units={"mf": "ppb"}, is_dequantified=False)

You can then access the Pint units directly:

.. jupyter-execute::

    co2_ppb_quantified.data["mf"].pint.units

.. note::
    Above mentioned unit conversion can be applied on ``get_obs_column``, ``get_flux``, ``get_footprint``, and ``get_bc`` too.

4. Cleanup
----------

If you're finished with the data in this tutorial you can cleanup the
tutorial object store using the ``clear_tutorial_store`` function.

.. jupyter-execute::

    from openghg.tutorial import clear_tutorial_store

.. jupyter-execute::

    clear_tutorial_store()
