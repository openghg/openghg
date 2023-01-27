Adding ancillary spatial data
=============================

This tutorial demonstrates how to add ancillary spatial data to the
OpenGHG store. These are split into several data types which currently
include:

-  “footprints” - regional outputs from an LPDM model (e.g. NAME)
-  “emissions” - estimates of species emissions/flux within a region
-  “boundary_conditions” - vertical curtains at the boundary of a
   regional domain
-  *“eulerian_model” - Global CTM output (e.g. GEOSChem)*

These inputs must adhere to an expected netcdf format and are expected
to minimally contain a fixed set of inputs.

*For some often used databases and inputs, there will be limited
transformation functions available to create and store data in this
format directly. See the ``openghg.transform`` sub-module for options.
To be expanded upon.*

Adding and standardising data
-----------------------------

Data can be added to the object store using appropriate functions from
the ``openghg.standardise`` sub module. This includes:

-  ``standardise_footprints``
-  ``standardise_flux``
-  ``standardise_bc``
-  *``standardise_eulerian`` - not implemented yet*

For all data types, as well as the path to the data file, a set of
keywords should be supplied. The applicable keywords depends upon the
data type itself but are used to categorise the data.

We can grab some data which is in the expected format (see below for
more details on the format) for our different types so these can be
added to the object store.

.. code:: ipython3

    from openghg.tutorial import retrieve_example_data
    
    fp_url = "https://github.com/openghg/example_data/raw/main/footprint/tac_footprint_inert_201607.tar.gz"
    data_file_fp = retrieve_example_data(url=fp_url)[0]
    
    flux_url = "https://github.com/openghg/example_data/raw/main/flux/ch4-ukghg-all_EUROPE_2016.tar.gz"
    data_file_flux = retrieve_example_data(url=flux_url)[0]
    
    bc_url = "https://github.com/openghg/example_data/raw/main/boundary_conditions/ch4_EUROPE_201607.tar.gz"
    data_file_bc = retrieve_example_data(url=bc_url)[0]


.. parsed-literal::

    Downloading tac_footprint_inert_201607.tar.gz: 100%|██████████| 67.0M/67.0M [00:17<00:00, 3.95MB/s]
    Downloading ch4-ukghg-all_EUROPE_2016.tar.gz: 100%|██████████| 82.5k/82.5k [00:00<00:00, 2.38MB/s]
    Downloading ch4_EUROPE_201607.tar.gz: 100%|██████████| 77.4k/77.4k [00:00<00:00, 4.22MB/s]


Data domains
^^^^^^^^^^^^

For spatial data, we can indicate consistent areas and resolution using
a *domain* as a label for this. For multiple pieces of data, a single
domain name should refer to the exact same set of latitude and longitude
bounds.

There are some pre-defined domain names already defined but it is also
possible to add new domain labels and definitions as needed. (*make sure
this has been consistently applied*)

*Add details for how to check known domains*

Footprints
~~~~~~~~~~

To standardise and store footprint data, in addition to the data file to
standardise, we also need to pass a set of keywords to label this. As a
minumum this needs to include: - ``site`` - site identifier (use
``openghg.standardise.summary_site_codes()`` function to check this) -
``inlet`` (/ height) associated with the site - ``domain`` - regional
domain covered by the footprint - ``model`` - name of model used to
create the footprint

Additional details can also be specified, in particular, for the
meteorological model used (metmodel) and the species name (if relevant).

For the example below, the footprint data generated from the NAME model
for the Tacolneston (TAC) site at 100m inlet. This covers an area over
Europe which we have defined as the “EUROPE” domain. Unless a specific
species is specified, this will be assumed to be a generic inert
species.

.. code:: ipython3

    from openghg.standardise import standardise_footprint
    
    standardise_footprint(data_file_fp, site="TAC", domain="EUROPE", inlet="100m", model="NAME")


.. parsed-literal::

    WARNING:openghg.store:This file has been uploaded previously with the filename : TAC-100magl_UKV_EUROPE_201607.nc - skipping.


This standardised data can then be accessed and retrieved from the
object store using the ``get_footprint`` function available from the
``openghg.retrieve`` submodule.

.. code:: ipython3

    from openghg.retrieve import get_footprint
    
    footprint_data = get_footprint(site="TAC", domain="EUROPE", inlet="100m")

For the standards associated the footprint files, there are also flags
which can be passed to sub-categorise the footprint inputs: -
``high_spatial_res`` - footprints containing multiple spatial
resolutions. This is usually an embedded high resolution region within a
larger lower resolution domain. - ``high_time_res`` - footprints which
include an additional dimension for resolving on the time axis. This is
associated with shorter term flux changes (e.g. natural sources of
carbon dioxide). A species will normally be associated with this
footprint (e.g. “co2”). - ``short_lifetime`` - footprints for species
with a shorter lifetime (<30 days). An explicit species input should be
associated with this footprint as well.

If possible, the standardise functionality will attempt to infer these
details but they should be supplied to ensure the footprint data is
labelled correctly. See schema details below for how these inputs are
defined.

Flux / Emissions
~~~~~~~~~~~~~~~~

To store and standardise flux / emissions data, as well as the input
file, as a minimum we need to supply the following keywords: -
``species`` - a name for the associated species - ``domain`` - the
regional domain covered by the flux data - ``source`` - a name for the
source of that data.

Optionally, additional identifiers can be use including: - ``database``
- inventory/database name associated with the flux data -
``database_version`` - if a database is specified, a version should be
included as well - ``model`` - the name of the model used to generate
the flux data

For the example below, the flux data is for methane (“ch4”) and which,
again, is covered by the same “EUROPE” domain as the footprint data
described above.

.. code:: ipython3

    from openghg.standardise import standardise_flux
    
    standardise_flux(data_file_flux, species="ch4", domain="EUROPE", source="anthro", model="ukghg")

.. code:: ipython3

    from openghg.retrieve import get_flux
    
    flux_data = get_flux(species="ch4", domain="EUROPE", source="anthro")

Boundary conditions
~~~~~~~~~~~~~~~~~~~

The boundary conditions data type describe the vertical curtains of a
regional domain. To store and standardise boundary conditions data, as
well as the input file, as a minimum we need to supply the following
keywords: - ``species`` - a name for the associated species - ``domain``
- the name of the domain the vertical curtains surround - ``bc_input`` -
a keyword descriptor for the boundary conditions inputs used

For the example below, the boundary conditions are for methane (“ch4”)
at the edges of the “EUROPE” domain. They were created using the *CAMS
climatology product [++REFINE AND ADD LINK++]*.

.. code:: ipython3

    from openghg.standardise import standardise_bc
    
    standardise_bc(data_file_bc, species="ch4", domain="EUROPE", bc_input="CAMS")


.. parsed-literal::

    WARNING:openghg.store:This file has been uploaded previously with the filename : ch4_EUROPE_201607.nc - skipping.


Defining ‘source’ and ‘bc_input’
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Unlike the ``domain`` and ``species`` inputs which have some pre-defined
values, the ``source`` and ``bc_input`` keywords can be chosen by the
user as a way to describe the flux and boundary condition inputs,
alongside the additional optional values. However, once a convention is
chosen for a given ``source`` or ``bc_input``, consistent keywords
should be used to describe like data so this can be associated and
distinguished correctly. Combinations of these keywords with the other
identifiers (such as species and domain) should allow associated data in
a timeseries to be identified.

See “Modifying_and_deleting_data” tutorial *[++ADD INTERNAL LINK++]* for
how to update stored metadata if needed.

To check the data and metadata already stored within an object store,
the ``search`` function from within the ``openghg.retrieve`` sub-module
can be used

.. code:: ipython3

    from openghg.retrieve import search
    
    search_results = search()
    search_results.results




.. raw:: html

    <div>
    <style scoped>
        .dataframe tbody tr th:only-of-type {
            vertical-align: middle;
        }
    
        .dataframe tbody tr th {
            vertical-align: top;
        }
    
        .dataframe thead th {
            text-align: right;
        }
    </style>
    <table border="1" class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>data_type</th>
          <th>processed by</th>
          <th>processed on</th>
          <th>raw file used</th>
          <th>species</th>
          <th>domain</th>
          <th>source</th>
          <th>author</th>
          <th>processed</th>
          <th>source_format</th>
          <th>...</th>
          <th>height</th>
          <th>spatial_resolution</th>
          <th>heights</th>
          <th>variables</th>
          <th>title</th>
          <th>date_created</th>
          <th>bc_input</th>
          <th>min_height</th>
          <th>max_height</th>
          <th>input_filename</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>emissions</td>
          <td>cv18710@bristol.ac.uk</td>
          <td>2021-01-08 12:18:49.803837+00:00</td>
          <td>/home/cv18710/work_shared/gridded_fluxes/ch4/u...</td>
          <td>ch4</td>
          <td>europe</td>
          <td>anthro</td>
          <td>openghg cloud</td>
          <td>2023-01-27 09:52:03.717769+00:00</td>
          <td>openghg</td>
          <td>...</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
        </tr>
        <tr>
          <th>1</th>
          <td>footprints</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>europe</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>...</td>
          <td>100m</td>
          <td>standard_spatial_resolution</td>
          <td>[500.0, 1500.0, 2500.0, 3500.0, 4500.0, 5500.0...</td>
          <td>[fp, temperature, pressure, wind_speed, wind_d...</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
        </tr>
        <tr>
          <th>2</th>
          <td>boundary_conditions</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>ch4</td>
          <td>europe</td>
          <td>NaN</td>
          <td>openghg cloud</td>
          <td>2023-01-27 11:45:22.736279+00:00</td>
          <td>NaN</td>
          <td>...</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>ecmwf cams ch4 volume mixing ratios at domain ...</td>
          <td>2018-11-13 09:25:29.112138</td>
          <td>cams</td>
          <td>500.0</td>
          <td>19500.0</td>
          <td>ch4_europe_201607.nc</td>
        </tr>
      </tbody>
    </table>
    <p>3 rows × 32 columns</p>
    </div>



To search for just one data type a specific ``search_*`` function can be
used (or the ``data_type`` input of ``"emissions"`` or
``"boundary_conditions"`` can be passed to the ``search`` function). For
example, for flux data:

.. code:: ipython3

    from openghg.retrieve import search_flux
    
    search_results_flux = search_flux()
    search_results_flux.results




.. raw:: html

    <div>
    <style scoped>
        .dataframe tbody tr th:only-of-type {
            vertical-align: middle;
        }
    
        .dataframe tbody tr th {
            vertical-align: top;
        }
    
        .dataframe thead th {
            text-align: right;
        }
    </style>
    <table border="1" class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>data_type</th>
          <th>processed by</th>
          <th>processed on</th>
          <th>raw file used</th>
          <th>species</th>
          <th>domain</th>
          <th>source</th>
          <th>author</th>
          <th>processed</th>
          <th>source_format</th>
          <th>start_date</th>
          <th>end_date</th>
          <th>max_longitude</th>
          <th>min_longitude</th>
          <th>max_latitude</th>
          <th>min_latitude</th>
          <th>time_resolution</th>
          <th>time_period</th>
          <th>uuid</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>emissions</td>
          <td>cv18710@bristol.ac.uk</td>
          <td>2021-01-08 12:18:49.803837+00:00</td>
          <td>/home/cv18710/work_shared/gridded_fluxes/ch4/u...</td>
          <td>ch4</td>
          <td>europe</td>
          <td>anthro</td>
          <td>openghg cloud</td>
          <td>2023-01-27 09:52:03.717769+00:00</td>
          <td>openghg</td>
          <td>2016-01-01 00:00:00+00:00</td>
          <td>2016-12-31 23:59:59+00:00</td>
          <td>39.38</td>
          <td>-97.9</td>
          <td>79.057</td>
          <td>10.729</td>
          <td>standard</td>
          <td>1 year</td>
          <td>b16fefdf-c92d-4cc9-8aac-367bcb6b82fe</td>
        </tr>
      </tbody>
    </table>
    </div>



In this case the ``source`` value has been set to ``"anthro"``.

Note that the ``source`` and ``bc_input`` keywords can also include “-”
to logically separate the descriptor e.g. “anthro-waste” but should not
include other separators.

Input format
------------

For each of these data types there is an associated object from the
``openghg.store`` sub-module:

-  ``Footprints``
-  ``Emissions``
-  ``BoundaryConditions``
-  *``EulerianModel`` - to be completed*

These objects can be used to give us information about the expected data
format using the ``.schema()`` method:

.. code:: ipython3

    from openghg.store import Emissions
    
    Emissions.schema()




.. parsed-literal::

    DataSchema(data_vars={'flux': ('time', 'lat', 'lon')}, dtypes={'lat': <class 'numpy.floating'>, 'lon': <class 'numpy.floating'>, 'time': <class 'numpy.datetime64'>, 'flux': <class 'numpy.floating'>}, dims=None)



This tells us that the netcdf input for “emissions” should contain: -
Data variables: - “flux” data variable with dimensions of (“time”,
“lat”, “lon”) - Data types: - “flux”, “lat”, “lon” variables /
coordinates should be float type - “time” coordinate should be
datetime64

Similarly for ``Footprints``, as described in the standardisations
section, there are a few different input options available: - inert
species (default - integrated footprint) - high spatial resolution
(``high_spatial_res`` flag) - high time resolution (``high_time_res``
flag) (e.g. for carbon dioxide) - short-lived species
(``short_lifetime`` flag) - particle locations (``particle_locations`` -
default is True, expect to be included)

Default (inert) footprint format
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These can be shown by passing keywords to the ``.schema()`` method. For
example, if nothing is passed this returns the details for an integrated
footprint for an inert species:

.. code:: ipython3

    from openghg.store import Footprints
    Footprints.schema()




.. parsed-literal::

    DataSchema(data_vars={'fp': ('time', 'lat', 'lon'), 'particle_locations_n': ('time', 'lon', 'height'), 'particle_locations_e': ('time', 'lat', 'height'), 'particle_locations_s': ('time', 'lon', 'height'), 'particle_locations_w': ('time', 'lat', 'height')}, dtypes={'lat': <class 'numpy.floating'>, 'lon': <class 'numpy.floating'>, 'time': <class 'numpy.datetime64'>, 'fp': <class 'numpy.floating'>, 'height': <class 'numpy.floating'>, 'particle_locations_n': <class 'numpy.floating'>, 'particle_locations_e': <class 'numpy.floating'>, 'particle_locations_s': <class 'numpy.floating'>, 'particle_locations_w': <class 'numpy.floating'>}, dims=None)



This tells us that the default netcdf input for “footprints” should
contain:

 - Data variables:
 
   - “fp” data variable with dimensions of (“time”, “lat”, “lon”)
   - “particle_locations_n”, “particle_locations_s” with dimensions of (“time”, “lon”, “height”)
   - “particle_locations_e”, “particle_locations_w” with dimensions of (“time”, “lat”, “height”)

 - Data types:
 
   - “fp”, “lat”, “lon”, “height” variables / coordinates should be float type
   - “particle_locations_n”, “particle_locations_e”, “particle_locations_s”, “particle_locations_w” variables should also be float type
   - “time” coordinate should be datetime64

The “fp” data variable describes the sensivity map within the regional
domain. The “particle_locations\_\*” variables describe the senitivity
map at each of the ordinal boundaries of the domain. Setting the
``particle_locations`` flag as False (True by default) would remove the
requirement for these particle location boundary sensitivies to be
included.

Other footprint formats
^^^^^^^^^^^^^^^^^^^^^^^

For species with a short lifetime the input footprints require
additional variables. This can be seen by passing the ``short_lifetime``
flag:

.. code:: ipython3

    Footprints.schema(short_lifetime=True)




.. parsed-literal::

    DataSchema(data_vars={'fp': ('time', 'lat', 'lon'), 'particle_locations_n': ('time', 'lon', 'height'), 'particle_locations_e': ('time', 'lat', 'height'), 'particle_locations_s': ('time', 'lon', 'height'), 'particle_locations_w': ('time', 'lat', 'height'), 'mean_age_particles_n': ('time', 'lon', 'height'), 'mean_age_particles_e': ('time', 'lat', 'height'), 'mean_age_particles_s': ('time', 'lon', 'height'), 'mean_age_particles_w': ('time', 'lat', 'height')}, dtypes={'lat': <class 'numpy.floating'>, 'lon': <class 'numpy.floating'>, 'time': <class 'numpy.datetime64'>, 'fp': <class 'numpy.floating'>, 'height': <class 'numpy.floating'>, 'particle_locations_n': <class 'numpy.floating'>, 'particle_locations_e': <class 'numpy.floating'>, 'particle_locations_s': <class 'numpy.floating'>, 'particle_locations_w': <class 'numpy.floating'>, 'mean_age_particles_n': <class 'numpy.floating'>, 'mean_age_particles_e': <class 'numpy.floating'>, 'mean_age_particles_s': <class 'numpy.floating'>, 'mean_age_particles_w': <class 'numpy.floating'>}, dims=None)



This tells us that, in addition to the “default” variables, for
short-lived species there also must be:

 - Additional data variables:
 
   - “mean_age_particles_n”, “mean_age_particles_s” with dimensions of (“time”, “lon”, “height”)
   - “mean_age_particles_e”, “mean_age_particles_w” with dimensions of (“time”, “lat”, “height”)
 
 - Data types:
 
   - all new variables should be float type

Similiarly for the ``high_time_res`` and ``high_spatial_res`` flags to
the ``Footprints.schema()`` method, these require additional variables
within the input footprint files.
