Export to dashboard
-------------------

.. note::
    This tutorial may be split up and modified based on the development of the AGAGE
    dashboard and standardisation functions.

In this short tutorial we'll cover the standardisation of AGAGE NetCDF
data created by
`agage-archive <https://github.com/mrghg/agage-archive>`__. We'll start
by importing what we need.

.. code:: ipython3

    from pathlib import Path
    from openghg.store import ObsSurface
    from openghg.standardise.surface import parse_agage
    from openghg.retrieve import search
    from openghg.objectstore import get_writable_bucket
    from openghg.util import to_dashboard

Now we'll need the path to the folder containing the AGAGE data

.. code:: ipython3

    data_folder = Path("/home/gareth/agage-public-archive-full")

Then we use the ``parse_agage`` function to read the NetCDF files, make
sure they meet our spec and read the metadata stored within their
attributes.

.. code:: ipython3

    standardised_data = parse_agage(data_folder=data_folder)

Adding data to object store
---------------------------

.. note::
    This section will be streamlined once changes to the handling of
    standardisation functions as part of in-progress PRs is complete.

We currently recommend creating a separate object store to house the
AGAGE data as the standardisation process and the metadata of the NetCDF
file is still changing frequently. To do this use
``openghg --quickstart`` from the command line.

We've created an object store called “agage” and will get
``get_writable_bucket`` to get the path to that store on our local
filesystem.

.. code:: ipython3

    bucket = get_writable_bucket(name="agage")

We're almost ready to pass the data to the ``ObsSurface.store_data``
function. We just need to set some required metadata keys to ensure the
data is assigned the Datasources correctly.

.. code:: ipython3

    required_keys = [ "inlet_latitude",
            "inlet_longitude",
            "species",
            "calibration_scale",
            "inlet_base_elevation_masl",
            "units",
            "site_code",
            "file_hash"]

.. code:: ipython3

    with ObsSurface(bucket=bucket) as obs:
        obs.store_data(data=standardised_data, required_metakeys=required_keys)

Retrieving the data
-------------------

Now we can retrieve the standardised data and export it to the JSON
format required by the dashboards.

.. code:: ipython3

    search_results = search(network="agage", store="agage")

Now we retrieve all the data as ``ObsData`` objects ready to pass to the
``to_dashboard`` function.

.. code:: ipython3

    agage_data = search_results.retrieve_all()

Exporting the data
------------------

We need to give the ``to_dashboard`` function a folder where it can
write a number of files. I'm going to create a folder in my home
directory called ``exported_dashboard_data`` and export the files to that.

.. code:: ipython3

    export_path = Path("/home/gareth/exported_dashboard_data")

We want to: 

* export to a temporary folder - ``data=agage_data`` 
* downsample the data, taking every 5th measurement - ``downsample_n=5`` 
* not compress the exported JSON - ``compress_json=False``
* drop NaNs - ``drop_na=True`` 
* tell the dashboard to allow selection of source by inlet - ``selection_level="inlet"``
* use a set inlet string for multi-inlet data (needed until the dashboard is updated and exported AGAGE data is tidied) - ``mock_inlet=True``

.. code:: ipython3

    to_dashboard(export_folder=export_path, 
                 data=agage_data, 
                 downsample_n=5, 
                 compress_json=False, 
                 drop_na=True, 
                 selection_level="inlet", 
                 mock_inlet=True)

You'll see a long list of messages telling you the names of the exported
files and possibly some messages telling you files are > 1 MB in size.
You'll also notice a message telling you the size of the complete data
package. When I ran this I got
``Total size of exported data package: 183.40 MB``. That's for 340
files, so on average 540 kB a file. Not bad but not great, we'll try and
do better later when we refine our export settings.

Exported files
--------------

If you have a look in the ``export_folder`` you'll see:

*  ``metadata_complete.json`` - a nested JSON file by the dashboards to
   construct their interface, know where to retrieve the data files and
   what metadata to display about the sites, instruments and inlets.
* ``dashboard_config.json`` - a JSON file used to tell the dashboards
   what kind of file compression and data saving techniques have been
   used and how users will interact with the app.
*  ``measurements/`` - a folder containing a JSON file for each of the
   sources (a specific inlet at a specific site or just a site itself), these may be compressed and/or
   have their floating point numbers converted to integers

We aim for each file in the ``measurements`` directory to be < 1 MB in
size. This is because the dashboard will only retrieve the metadata and
a single source's data on first load. As the user then selects other
sites and species it will retrieve each source's file on the fly. The
smaller the file the more responsive the dashboard will be to users on
slower connections.

Updating the dashboard
----------------------

To update the dashboard check their respective developer guides at...