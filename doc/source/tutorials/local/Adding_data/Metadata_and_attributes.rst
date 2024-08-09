Metadata and attributes
=======================

*Note at the moment this tutorial is only applicable to surface data and not to the other data types.*

0. Using the tutorial object store
----------------------------------

To avoid adding the example data we use in this tutorial to your normal
object store, we need to tell OpenGHG to use a separate sandboxed object
store that we'll call the tutorial store. To do this we use the
``use_tutorial_store`` function from ``openghg.tutorial``. This sets the
``OPENGHG_TUT_STORE`` environment variable for this session and won't
affect your use of OpenGHG outside of this tutorial.

.. code:: ipython3

    from openghg.tutorial import use_tutorial_store

    use_tutorial_store()


1. Add example observation data
-------------------------------

For this tutorial, we can add some example Tacolnestion data to the tutorial object store.

.. code:: ipython3

    from openghg.tutorial import retrieve_example_data

    data_url = "https://github.com/openghg/example_data/raw/main/timeseries/tac_example.tar.gz"

    tac_data = retrieve_example_data(url=data_url)

.. code:: ipython3

    from openghg.standardise import standardise_surface

    standardise_surface(filepaths=tac_data, source_format="CRDS", site="TAC", network="DECC")

We'll also retrieve some dummy alternative data which we can use to demonstrate how to deal with
mismatching metadata and attributes.

.. code:: ipython3

    from openghg.tutorial import retrieve_example_data

    data_url = "https://github.com/openghg/example_data/raw/main/timeseries/tac_dummy_attr_mismatch_example.tar.gz"

    dummy_data = retrieve_example_data(url=data_url)

We will demonstrate how to add this to the object store below.

2. Metadata and attributes
--------------------------

Within OpenGHG, metadata is used as a way to categorise a data source by
applying a unique set of keys. This is used to distinguish between stored data
and allows this to be searchable.

For CF-compliance and ease of use, we also provide a set of internal attributes
stored alongside the data and within the netcdf file (as Dataset.attrs when using
xarray for instance).

The *metadata* and *attributes* stored do not necessarily need to be the same for
a given data source. Indeed, we may want different information stored as tags in
the metadata and more extensive details included in the attributes. However, for any
overlapping tags (keys) between the metadata and the attributes these values **must match**.

For the current observation data we have available we can retrieve the carbon dioxide data for the  Tacolneston Site
at the 185m inlet.

.. code:: ipython3

    from openghg.retrieve import get_obs_surface

    co2_data = get_obs_surface(site="tac", species="co2", inlet="185m")

Previously, we have focussed on the `.data` attribute to access the stored data for this output.
We can access the metadata details in a similiar way by looking at the `.metadata` attribute:

.. code:: ipython3

    co2_data.metadata

Output:

.. code::

    {'data_type': 'surface',
     'site': 'tac',
     'instrument': 'picarro',
     'sampling_period': '3600.0',
     'inlet': '185m',
     'port': '10',
     'type': 'air',
     'network': 'decc',
     'species': 'co2',
     'calibration_scale': 'wmo-x2007',
     'long_name': 'tacolneston',
     'inlet_height_magl': '185m',
     'data_owner': "Simon O'Doherty",
     'data_owner_email': 's.odoherty@bristol.ac.uk',
     'station_longitude': 1.13872,
     'station_latitude': 52.51775,
     'station_long_name': 'Tacolneston Tower, UK',
     'station_height_masl': 50.0,
     'uuid': 'f3e1ef46-8907-4096-8215-19bd6e1c55e3',
     'comment': 'Cavity ring-down measurements. Output from GCWerks',
     'conditions_of_use': 'Ensure that you contact the data owner at the outset of your project.',
     'source': 'In situ measurements of air',
     'Conventions': 'CF-1.8',
     'file_created': '2022-12-13 10:23:34.956121+00:00',
     'processed_by': 'OpenGHG_Cloud',
     'sampling_period_unit': 's',
     'scale': 'WMO-X2007'}

You will see this is stored as a dictionary containing the unique keys associated with this data.
These details are is what allows openghg to search and retrieve specific data sources.

The attributes are associated internally with the data itself:

.. code:: ipython3

    co2_ds = co2_data.data
    co2_ds

Output:

.. code::

    xarray.Dataset
        Dimensions:
        time: 39114
        Coordinates:
        time
        (time)
        datetime64[ns]
        2013-01-31T00:13:28 ... 2017-12-...
        Data variables:
            mf             (time) float64 401.6 403.4 403.1 ... 411.1 411.1
            mf_variability (time) float64 0.155 0.088 0.204 ... 0.421 0.325
            mf_number_...  (time) float64 259.0 251.0 252.0 ... 596.0 596.0
        Indexes: (1)
        Attributes: (25)

To access the "Attributes" we can use the `.attrs` keyword for xarray Datasets.

.. code:: ipython3

    co2_ds.attrs

Output:

.. code:: 

    {'data_type': 'surface',
    'site': 'tac',
    'instrument': 'picarro',
    'sampling_period': '3600.0',
    'inlet': '185m',
    'port': '10',
    'type': 'air',
    'network': 'decc',
    'species': 'co2',
    'calibration_scale': 'wmo-x2007',
    'long_name': 'tacolneston',
    'inlet_height_magl': '185m',
    'data_owner': "Simon O'Doherty",
    'data_owner_email': 's.odoherty@bristol.ac.uk',
    'station_longitude': 1.13872,
    'station_latitude': 52.51775,
    'station_long_name': 'Tacolneston Tower, UK',
    'station_height_masl': 50.0,
    'uuid': 'f3e1ef46-8907-4096-8215-19bd6e1c55e3',
    'comment': 'Cavity ring-down measurements. Output from GCWerks',
    'conditions_of_use': 'Ensure that you contact the data owner at the outset of your project.',
    'source': 'In situ measurements of air',
    'Conventions': 'CF-1.8',
    'file_created': '2022-12-13 10:23:34.956121+00:00',
    'processed_by': 'OpenGHG_Cloud',
    'sampling_period_unit': 's',
    'scale': 'WMO-X2007'}

Storing attributes in this way means it's easy to create a CF-compliant netcdf file from the standardised
data in the object store, for example using the `.to_netcdf()` method on our Dataset:

.. code:: ipython3

    # co2_ds.to_netcdf(...)

Here we would uncomment this and substitue `...` for a filepath.

3. Resolving mismatches
-----------------------

When the *metadata* and *attributes* are created as part of the openghg standardisation
process (and when using `retrieve_atmospheric`), these sets of details are often collated
from different sources.

In general:

* attributes are drawn from internal attributes from the data
* metadata is drawn from additional external details including user inputs
  and the `openghg/openghg_defs <https://github.com/openghg/openghg_defs/tree/main/openghg_defs/data>`_
 data repository.

Depending on the standardisation procedure, there are cases where there may be a mismatch between
these two sets of details. For instance, you may wish to specify a station long name when adding a new data file
as an input but this conflicts with attributes stored within the data file itself. You may also find when
retrieving data from an external source, such as the ICOS Carbon Portal, the
attributes stored alongside retrieved data do not match to our definitions stored within the
openghg/openghg_defs `site_info details <https://github.com/openghg/openghg_defs/blob/main/openghg_defs/data/site_info.json>`_
for that site.

Though overlapping details stored in the attributes and metadata must match, how these mismatches are
handled is up to the user. When adding new data either via `standardise_surface` (or pulling data using `retrieve_atmopsheric`)
this can be done through the `update_mismatch` keyword.

In Step 1, you should have already retrieved some dummy data we can use to demonstrate this. This will have
been created as a variable called `dummy_data` which we will use below. Check this has been
run if you're unable to access this variable.

.. code:: ipython3

    standardise_surface(filepaths=dummy_data,
                        source_format="openghg",
                        site="TAC",
                        network="DECC",
                        inlet="998m",
                        instrument="picarro",
                        sampling_period="1H")

Output::
 
    ---------------------------------------------------------------------------
    AttrMismatchError                         Traceback (most recent call last)

    ...

    AttrMismatchError: Metadata mismatch / value not within tolerance for the following keys:
    - 'station_long_name', metadata: Tacolneston Tower, UK, attributes: ATTRIBUTE DATA
    - 'station_height_masl', metadata: 64, attributes: 50.0
    Output is truncated. View as a scrollable element or open in a text editor. Adjust cell output settings...

If we try to add this dummy data, we'll see that this fails with a `AttrMismatchError`. This is because some details stored
within the input file (the attributes) don't match to the details within our stored data within the openghg/openghg_defs
[site_info details](https://github.com/openghg/openghg_defs/blob/main/openghg_defs/data/site_info.json)
for that site. By default `update_mismatch` is set to `"never"` which means this will produce an error
rather than guessing how to resolve this.

The error message above also tells us what doesn't match:

* station_long_name

   * metadata: Tacolneston Tower, UK

   * attributes: ATTRIBUTE DATA

* station_height_masl

   * metadata: 64

   * attributes: 50.0

We can choose how we want to resolve this using the options for the `update_mismatch` keyword:

* "from_source" (or "attributes") - use the value(s) included within the current attributes
* "from_definition" (or "metadata") - use the value(s) included within the current metadata

In this case, we choose to use the details from the metadata (derived from site_info details)
by running `standardise_surface` again but this time using `update_mismatch="from_definition"`.

.. code:: ipython3

    standardise_surface(filepaths=dummy_data,
                        source_format="openghg",
                        site="TAC",
                        species="co2",
                        network="DECC",
                        inlet="998m",
                        instrument="picarro",
                        sampling_period="1H",
                        update_mismatch="from_definition")

This should now run without error (warnings will be printed and logged instead).

.. code:: ipython3

    dummy_data = get_obs_surface(site="tac", species="co2", inlet="998m")

We can look at the `station_long_name` stored within the metadata:

.. code:: ipython3

    dummy_data.metadata["station_long_name"]

Output:

.. code::

    'Tacolneston Tower, UK'

and attributes:

.. code:: ipython3

    dummy_data.data.attrs["station_long_name"]

Output:

.. code:: 

    'Tacolneston Tower, UK'

to check this is what we expected.

4. Cleanup
----------

If you're finished with the data in this tutorial you can cleanup the
tutorial object store using the ``clear_tutorial_store`` function.

.. code:: ipython3

    from openghg.tutorial import clear_tutorial_store

.. code:: ipython3

    clear_tutorial_store()
