Retrieving data from remote archives
====================================

This tutorial covers the retrieval of data from the
`ICOS Carbon Portal <https://www.icos-cp.eu/observations/carbon-portal>`__ [#f1]_
and the `CEDA archives <https://data.ceda.ac.uk/badc>`__ [#f2]_.

Using the tutorial object store
-------------------------------

As in the :ref:`previous tutorial <using-the-tutorial-object-store>`,
we will use the tutorial object store to avoid cluttering your personal
object store.

.. code-block:: ipython3

    In [1]: from openghg.tutorial import use_tutorial_store

    In [1]: use_tutorial_store()

1. ICOS
-------

It's easy to retrieve atmospheric gas measurements from the `ICOS Carbon
Portal`_  using OpenGHG. To do so we'll use the ``retrieve_atmospheric``
function from ``openghg.retrieve.icos``.

.. _`ICOS Carbon Portal`: https://www.icos-cp.eu/observations/carbon-portal

Checking available data
~~~~~~~~~~~~~~~~~~~~~~~

You can find the stations available in ICOS using `their map
interface`_.
Click on a site to see its information, then use its three letter site
code to retrieve data.
You can also use the `search page`_ to find available data at a given site.

.. _`their map interface`: https://data.icos-cp.eu/portal/#%7B%22filterCategories%22%3A%7B%22project%22%3A%5B%22icos%22%5D%2C%22level%22%3A%5B1%2C2%5D%2C%22stationclass%22%3A%5B%22ICOS%22%5D%2C%22theme%22%3A%5B%22atmosphere%22%5D%7D%2C%22tabs%22%3A%7B%22resultTab%22%3A2%7D%7D

.. _`search page`: https://data.icos-cp.eu/portal/#%7B%22filterCategories%22:%7B%22project%22:%5B%22icos%22%5D,%22level%22:%5B1,2%5D,%22stationclass%22:%5B%22ICOS%22%5D%7D%7D

Using ``retrieve_atmospheric``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First we'll import ``retrieve_atmospheric`` from the ``retrieve`` submodule, then
we'll retrieve some data from Saclay (**SAC**). The function will
first check for any data from **SAC** already stored in the object
store, if any is found it is returned, otherwise it'll retrieve the data
from the ICOS Carbon Portal, this may take a bit longer.

.. ipython::
   :verbatim:

   In [1]: from openghg.retrieve.icos import retrieve_atmospheric

.. ipython::
    :verbatim:

    In [2]: sac_data = retrieve_atmospheric(site="SAC", species="ch4", sampling_height="100m")

    In [3]: len(sac_data)
    Out[3]: 2

Here ``sac_data`` is a list of two ``ObsData`` objects, each containing differing amounts of data.
We can see why there are two versions of this data by checking the ``dataset_source`` key
in the attached metadata.

.. ipython::
    :verbatim:

    In [7]: dataset_sources = [obs.metadata["dataset_source"] for obs in sac_data]

    In [8]: dataset_sources
    Out[8]: ['ICOS', 'European ObsPack']



Let's say we want to look at the ICOS dataset, we can select that first dataset

.. ipython::
    :verbatim:

    In [9]: sac_data_icos = sac_data[0]

    In [11]: sac_data_icos
    Out[11]:
    ObsData(data=<xarray.Dataset>
    Dimensions:                     (time: 40510)
    Coordinates:
    * time                        (time) datetime64[ns] 2017-05-31 ... 2022-02-...
    Data variables:
        flag                        (time) object 'O' 'O' 'O' 'O' ... 'O' 'O' 'O'
        ch4_number_of_observations  (time) int64 11 11 11 3 11 11 ... 12 12 12 12 12
        ch4_variability             (time) float64 1.551 5.315 15.57 ... 0.508 2.524
        ch4                         (time) float64 1.935e+03 1.938e+03 ... 2.05e+03
    Attributes: (12/33)
        species:                ch4
        instrument:             RAMCES - G24
        instrument_data:        ['RAMCES - G24', 'http://meta.icos-cp.eu/resource...
        site:                   SAC
        measurement_type:       ch4 mixing ratio (dry mole fraction)
        units:                  nmol mol-1
        ...                     ...
        Conventions:            CF-1.8
        file_created:           2023-06-14 12:52:11.547608+00:00
        processed_by:           OpenGHG_Cloud
        calibration_scale:      unknown
        sampling_period:        NOT_SET
        sampling_period_unit:   s, metadata={'station_long_name': 'sac', 'station_latitude': 48.7227, 'station_longitude': 2.142, 'species': 'ch4', 'network': 'icos', 'data_type': 'surface', 'data_source': 'icoscp', 'source_format': 'icos', 'icos_data_level': '2', 'site': 'sac', 'inlet': '100m', 'inlet_height_magl': '100', 'instrument': 'ramces - g24', 'sampling_period': 'not_set', 'calibration_scale': 'unknown', 'data_owner': 'morgan lopez', 'data_owner_email': 'morgan.lopez@lsce.ipsl.fr', 'station_height_masl': 160.0, 'dataset_source': 'ICOS'})


We can see that we've retrieved ``ch4`` data that covers 2021-07-01 -
2022-02-28. A lot of metadata is stored during the retrieval
process, including where the data was retrieved from (``dobj_pid`` in
the metadata), the instruments, their associated metadata and a
citation string.

You can see more information about the instruments by going to the link
in the ``instrument_data`` section of the metadata

.. ipython::
    :verbatim:

    In [14]: metadata = sac_data_icos.metadata

    In [15]: metadata["instrument_data"]

    In [16]: metadata["citation_string"]

Here we get the instrument name and a link to the instrument data on the
ICOS Carbon Portal.

Viewing the data
~~~~~~~~~~~~~~~~

As with any ``ObsData`` object we can quickly plot it to have a look.

   **NOTE:** the plot created below may not show up on the online
   documentation. If you're using an `ipython` console to run through the tutorial,
   the plot will open in a new browser window.

.. ipython::
    :verbatim:

    In [17]:  sac_data_icos.plot_timeseries()

Data levels
~~~~~~~~~~~

Data available on the ICOS Carbon Portal is made available under three
different levels (`see
docs <https://icos-carbon-portal.github.io/pylib/modules/#stationdatalevelnone>`__).

- Data level 1: Near Real Time Data (NRT) or Internal Work data (IW).
- Data level 2: The final quality checked ICOS RI data set, published by the CFs,
  to be distributed through the Carbon Portal. This level is the ICOS-data product
  and free available for users.
- Data level 3: All kinds of elaborated products by scientific communities that
  rely on ICOS data products are called Level 3 data.

By default level 2 data is retrieved but this can be changed by passing
``data_level`` to ``retrieve_icos``.
Note that level 1 data may not have been quality checked.

Below we'll retrieve some more recent data from **SAC**.

.. ipython::
    :verbatim:

    In [2]: sac_data_level1 = retrieve_atmospheric(site="SAC", species="CH4", sampling_height="100m", data_level=1, dataset_source="icos")

    In [4]: sac_data_level1.data.time[0]

    In [7]: sac_data_level1.data.time[-1]

You can see that we've now got quite recent data, usually up until a day or so before these docs were built. The
ability to retrieve different level data has been added for convenience, choose the best option for your workflow.

.. ipython::
    :verbatim:

    In [10]: sac_data_level1.plot_timeseries(title="SAC - Level 1 data")

Forcing retrieval
~~~~~~~~~~~~~~~~~

As ICOS data is cached by OpenGHG you may sometimes need to force a
retrieval from the ICOS Carbon Portal.

If you retrieve data using ``retrieve_icos`` and notice that it does not
return the most up to date data (compare the dates with those on the
portal) you can force a retrieval using ``force_retrieval``.

.. ipython::
    :verbatim:

    In [11]: new_data = retrieve_atmospheric(site="SAC", species="CH4", data_level=1, force_retrieval=True)

Here we get a message telling us there is no new data to
process, this will depend on the rate at which datasets are updated on the ICOS Carbon Portal.

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

.. ipython::
    :verbatim:

    In [1]: from openghg.retrieve.ceda import retrieve_surface

    In [2]: url = "https://dap.ceda.ac.uk/badc/gauge/data/tower/heathfield/co2/100m/bristol-crds_heathfield_20130101_co2-100m.nc?download=1"

    In [3]: hfd_data = retrieve_surface(url=url)

    In [4]: hfd_data
    Out[4]:
    ObsData(data=<xarray.Dataset>
    Dimensions:                     (time: 955322)
    Coordinates:
      * time                        (time) datetime64[ns] 2013-11-20T12:51:30 ......
    Data variables:
      co2                         (time) float64 401.4 401.4 401.5 ... 409.2 409.1
      co2_variability             (time) float64 0.075 0.026 0.057 ... 0.031 0.018
      co2_number_of_observations  (time) float64 19.0 19.0 20.0 ... 19.0 19.0 19.0
    Attributes: (12/21)
      comment:              Cavity ring-down measurements. Output from GCWerks
      Source:               In situ measurements of air
      Processed by:         Aoife Grant, University of Bristol (aoife.grant@bri...
      data_owner_email:     s.odoherty@bristol.ac.uk
      data_owner:           Simon O'Doherty
      inlet_height_magl:    100.0
      ...                   ...
      data_type:            surface
      data_source:          ceda_archive
      network:              CEDA_RETRIEVED
      sampling_period:      NA
      site:                 hfd
      inlet:                100m, metadata={'comment': 'Cavity ring-down measurements. Output from GCWerks', 'Source': 'In situ measurements of air', 'Processed by': 'Aoife Grant, University of Bristol (aoife.grant@bristol.ac.uk)', 'data_owner_email': 's.odoherty@bristol.ac.uk', 'data_owner': "Simon O'Doherty", 'inlet_height_magl': 100.0, 'Conventions': 'CF-1.6', 'Conditions of use': 'Ensure that you contact the data owner at the outset of your project.', 'File created': '2018-10-22 16:05:33.492535', 'station_long_name': 'Heathfield, UK', 'station_height_masl': 150.0, 'station_latitude': 50.97675, 'station_longitude': 0.23048, 'Calibration_scale': 'NOAA-2007', 'species': 'co2', 'data_type': 'surface', 'data_source': 'ceda_archive', 'network': 'CEDA_RETRIEVED', 'sampling_period': 'NA', 'site': 'hfd', 'inlet': '100m'})

Now we've got the data, we can use it as any other ``ObsData`` object,
using ``data`` and ``metadata``.

.. ipython::
    :verbatim:

    In [4]: hfd_data.plot_timeseries()

Within an ``ipython`` session the plot will be opened in a new window, in a notebook it will appear in the cell below.

Retrieving a second time
~~~~~~~~~~~~~~~~~~~~~~~~

The second time we (or another user) retrieves the data it will be pulled
from the object store, this should be faster than retrieving from CEDA.
To get the same data again use the ``site``, ``species`` and ``inlet``
arguments.

.. ipython::
    :verbatim:

    In [6]: hfd_data_ceda = retrieve_surface(site="hfd", species="co2")

    In [7]: hfd_data_ceda


3. Cleanup
----------

If you're finished with the data in this tutorial you can cleanup the
tutorial object store using the ``clear_tutorial_store`` function.

.. ipython::
    :verbatim:

    In [8]: from openghg.tutorial import clear_tutorial_store

.. ipython::
    :verbatim:

    In [9]: clear_tutorial_store()
    INFO:openghg.tutorial:Tutorial store at /home/gareth/openghg_store/tutorial_store cleared.


.. FOOTNOTES
.. ---------

.. rubric:: Footnotes

.. [#f1] ICOS means *Integrated Carbon Observation System*. See `ICOS in a nutshell <https://www.icos-cp.eu/about/icos-in-nutshell>`__.

.. [#f2] CEDA means *Centre for Environmental Data Analysis*. See their `homepage <https://www.ceda.ac.uk/about/what-we-do/>`__.
