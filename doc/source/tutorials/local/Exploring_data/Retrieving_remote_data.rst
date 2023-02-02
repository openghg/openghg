Retrieving data from remote archives
====================================

This tutorial covers the retrieval of data from the ICOS Carbon Portal
and the CEDA archives.

0. Using the tutorial object store
----------------------------------

To avoid adding the example data we use in this tutorial to your normal
object store, we need to tell OpenGHG to use a separate sandboxed object
store that we'll call the tutorial store. To do this we use the
``use_tutorial_store`` function from ``openghg.tutorial``. This sets the
``OPENGHG_TUT_STORE`` environment variable for this session and won't
affect your use of OpenGHG outside of this tutorial.

.. ipython::

    In [1]: from openghg.tutorial import use_tutorial_store

    In [1]: use_tutorial_store()

1. ICOS
-------

It's easy to retrieve atmospheric gas measurements from the `ICOS Carbon
Portal <https://www.icos-cp.eu/observations/carbon-portal>`__ using
OpenGHG. To do so we'll use the ``retrieve_atmospheric`` function from
``openghg.retrieve.icos``.

Checking available data
~~~~~~~~~~~~~~~~~~~~~~~

You can find the stations available in ICOS using `their map
interface <https://data.icos-cp.eu/portal/#%7B%22filterCategories%22%3A%7B%22project%22%3A%5B%22icos%22%5D%2C%22level%22%3A%5B1%2C2%5D%2C%22stationclass%22%3A%5B%22ICOS%22%5D%2C%22theme%22%3A%5B%22atmosphere%22%5D%7D%2C%22tabs%22%3A%7B%22resultTab%22%3A2%7D%7D>`__.
Click on a site to see it's information, then use it's three letter site
code to retrieve data. You can also use the `search
page <https://data.icos-cp.eu/portal/#%7B%22filterCategories%22:%7B%22project%22:%5B%22icos%22%5D,%22level%22:%5B1,2%5D,%22stationclass%22:%5B%22ICOS%22%5D%7D%7D>`__
to find available data at a given site.

Using ``retrieve_atmospheric``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First we'll import ``retrieve_atmospheric`` from the ``retrieve`` submodule, then
we'll retrieve some data from Weybourne (**WAO**). The function will
first check for any data from **WAO** already stored in the object
store, if any is found it is returned, otherwise it'll retrieve the data
from the ICOS Carbon Portal, this may take a bit longer.

.. ipython::

   In [1]: from openghg.retrieve.icos import retrieve_atmospheric

.. ipython::

    In [2]: wao_data = retrieve_atmospheric(site="WAO", species="ch4", sampling_height="10m")
    In [3]: len(wao_data)
    Out[3]: 3

Here `wao_data` is a list of three `ObsData` objects, each one containing differing amounts of data.
We can have a look at the reason for their being three versions of data by checking the `dataset_source` key
in the attached metadata.

.. ipython::

    In [7]: dataset_sources = [obs.metadata["dataset_source"] for obs in wao_data]

    In [8]: dataset_sources
    Out[8]: ['ICOS', 'InGOS', 'European ObsPack']

Let's say we want to look at ICOS dataset, we can

.. ipython::

    In [9]: wao_data_icos = wao_data[0]
    In [11]: wao_data_icos.data
    Out[11]:
        <xarray.Dataset>
        Dimensions:                     (time: 4883)
        Coordinates:
        * time                        (time) datetime64[ns] 2021-07-01 ... 2022-02-...
        Data variables:
            flag                        (time) object 'O' 'O' 'O' 'O' ... 'O' 'O' 'O'
            ch4_number_of_observations  (time) int64 59 59 59 58 60 ... 59 60 60 60 58
            ch4_variability             (time) float64 0.277 0.373 1.646 ... 1.332 3.087
            ch4                         (time) float64 1.951e+03 1.952e+03 ... 2.103e+03
        Attributes: (12/34)
            species:                ch4
            instrument:             FTIR
            instrument_data:        ['FTIR', 'http://meta.icos-cp.eu/resources/instru...
            site:                   WAO
            measurement_type:       ch4 mixing ratio (dry mole fraction)
            units:                  nmol mol-1
            ...                     ...
            Conventions:            CF-1.8
            file_created:           2023-02-02 15:25:52.075469+00:00
            processed_by:           OpenGHG_Cloud
            calibration_scale:      unknown
            sampling_period:        NOT_SET
            sampling_period_unit:   s

We can see that we've retrieved ``ch4`` data that covers 2021-07-01 -
2022-02-28. A lot of metadata is stored during the retrieval
process, including where the data was retrieved from (``dobj_pid`` in
the metadata), the instruments, their associated metadata and a
citation string.

You can see more information about the instruments by going to the link
in the ``instrument_data`` section of the metadata

.. ipython::

    In [14]: metadata = wao_data_icos.metadata
    ...: instrument_data = metadata["instrument_data"]
    ...: citation_string = metadata["citation_string"]

    In [15]: instrument_data
    Out[15]: ['FTIR', 'http://meta.icos-cp.eu/resources/instruments/ATC_505']

    In [16]: citation_string
    Out[16]: 'Forster, G., Manning, A. (2022). ICOS ATC CH4 Release, Weybourne (10.0 m), 2021-07-01–2022-02-28, ICOS RI, https://hdl.handle.net/11676/LmhTdKx6FLGwplSh2tAIGGLj'

Here we get the instrument name and a link to the instrument data on the
ICOS Carbon Portal.

Viewing the data
~~~~~~~~~~~~~~~~

As with any ``ObsData`` object we can quickly plot it to have a look.

   **NOTE:** the plot created below may not show up on the online
   documentation. If you're using an `ipython` console to run through the tutorial,
   the plot will open in a new browser window.

.. ipython::

   In [17]:  wao_data_icos.plot_timeseries()

Data levels
~~~~~~~~~~~

Data available on the ICOS Carbon Portal is made available under three
different levels (`see
docs <https://icos-carbon-portal.github.io/pylib/modules/#stationdatalevelnone>`__).

::

   - Data level 1: Near Real Time Data (NRT) or Internal Work data (IW).
   - Data level 2: The final quality checked ICOS RI data set, published by the CFs,
                   to be distributed through the Carbon Portal.
                   This level is the ICOS-data product and free available for users.
   - Data level 3: All kinds of elaborated products by scientific communities
                   that rely on ICOS data products are called Level 3 data.

By default level 2 data is retrieved but this can be changed by passing
``data_level`` to ``retrieve_icos``. Below we'll retrieve some more
recent data from **WAO**.

.. ipython::

    In [2]:  wao_data_level1 = retrieve_atmospheric(site="WAO",
    ...:                                             species="CH4",
    ...:                                             sampling_height="10m",
    ...:                                             data_level=1,
    ...:                                             dataset_source="icos")
    In [4]: wao_data_level1.data.time[0]
    Out[4]:
    <xarray.DataArray 'time' ()>
    array('2022-03-01T00:00:00.000000000', dtype='datetime64[ns]')
    Coordinates:
        time     datetime64[ns] 2022-03-01

    In [7]: wao_data_level1.data.time[-1]
    Out[7]:
    <xarray.DataArray 'time' ()>
    array('2023-02-01T22:00:00.000000000', dtype='datetime64[ns]')
    Coordinates:
        time     datetime64[ns] 2023-02-01T22:00:00
    ...

You can see that we've now got data from 2022-03-01 - 2023-02-01. The
ability to retrieve different level data has been added for convenience,
choose the best option for your workflow.

   **NOTE:** level 1 data may not have been quality checked.

.. ipython::

    In [10]: wao_data_level1.plot_timeseries(title="WAO - Level 1 data")

Forcing retrieval
~~~~~~~~~~~~~~~~~

As ICOS data is cached by OpenGHG you may sometimes need to force a
retrieval from the ICOS Carbon Portal.

If you retrieve data using ``retrieve_icos`` and notice that it does not
return the most up to date data (compare the dates with those on the
portal) you can force a retrieval using ``force_retrieval``.

.. ipython::

    In [11]: new_data = retrieve_atmospheric(site="WAO", species="CH4", data_level=1, force_retrieval=True)
    WARNING:openghg.store:Note: There is no new data to process.

Here you may notice we get a message telling us there is no new data to
process, if you force a retrieval and there is no newer data you'll see
this message.

2. CEDA
-------

To retrieve data from CEDA you can use the ``retrieve_surface`` function
from ``openghg.retrieve.ceda``. This lets you pull down data from CEDA, process
it and store it in the object store. Once the data has been stored
successive calls will retrieve the data from the object store.

   **NOTE:** For the moment only surface observations can be retrieved
   and it is expected that these are already in a NetCDF file. If you
   find a file that can't be processed by the function please `open an
   issue on
   GitHub <https://github.com/openghg/openghg/issues/new/choose>`__ and
   we'll do our best to add support that file type.

To pull data from CEDA you'll first need to find the URL of the data. To
do this use the `CEDA data browser <https://data.ceda.ac.uk/badc>`__ and
copy the link to the file (right click on the download button and click
copy link / copy link address). You can then pass that URL to
``retrieve_surface``, it will then download the data, do some
standardisation and checks and store it in the object store.

We don't currently support downloading restricted data that requires a
login to access. If you'd find this useful please open an issue at the
link given above.

Now we're ready to retrieve the data.

.. code:: ipython3

    from openghg.retrieve.ceda import retrieve_surface

.. code:: ipython3

    url = "https://dap.ceda.ac.uk/badc/gauge/data/tower/heathfield/co2/100m/bristol-crds_heathfield_20130101_co2-100m.nc?download=1"

.. code:: ipython3

    hfd_data = retrieve_surface(url=url)

Now we've got the data, we can use it as any other ``ObsData`` object,
using ``data`` and ``metadata``.

.. code:: ipython3

    hfd_data.plot_timeseries()

Retrieving a second time
~~~~~~~~~~~~~~~~~~~~~~~~

The second time we (or another user) retrieves the data it will be pulled
from the object store, this should be faster than retrieving from CEDA.
To get the same data again use the ``site``, ``species`` and ``inlet``
arguments.

.. code:: ipython3

    hfd_data2 = retrieve_surface(site="hfd", species="co2")

.. code:: ipython3

    hfd_data2

3. Cleanup
----------

If you're finished with the data in this tutorial you can cleanup the
tutorial object store using the ``clear_tutorial_store`` function.

.. code:: ipython3

    from openghg.tutorial import clear_tutorial_store

.. code:: ipython3

    clear_tutorial_store()
