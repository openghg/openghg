Retrieve original files
=======================

Each file that is standardised by OpenGHG is hashed, compressed and stored in the object store.
We do this to ensure data traceability and to allow us to re-standardise the data in case issues are found
in the process. In this tutorial we'll standardise some footprint data, search for it in
the object store and then retrieve the original files.

Standardise footprint data
--------------------------

First let's download some example footprint data from our example data repository.

.. code:: ipython3

    from openghg.tutorial import retrieve_example_data

    fp_url = "https://github.com/openghg/example_data/raw/main/footprint/tac_footprint_inert_201607.tar.gz"
    data_file_fp = retrieve_example_data(url=fp_url)[0]

    Downloading tac_footprint_inert_201607.tar.gz: 100%|██████████| 67.0M/67.0M [00:17<00:00, 3.95MB/s]

Next we'll standardise the footprint using ``standardise_footprint`` and make sure it's stored
in our ``user`` object store.

.. code:: ipython3

    from openghg.standardise import standardise_footprint

    standardise_footprint(data_file_fp, site="TAC", domain="EUROPE", inlet="100m", model="NAME", store="user")

    [16/04/24 09:04:18] INFO     INFO:openghg.standardise.footprint:Rechunking with chunks={'lat': 293, 'lon': 391, 'time': 480}                  _acrg_org.py:60
    {'tac_europe_NAME_100m': {'uuid': '582cf61e-0dcf-46ce-b048-771474c738a4', 'new': True}}

The standardisation process tells us that the footprint data will be rechunked for storage
and then gives us the UUID of the Datasource the data is stored by.


Find the data
-------------

Now let's find that data in the object store using the `search_footprints` function

.. code-block:: ipython3

    from openghg.retrieve import search_footprints

    results = search_footprints(site="TAC", inlet="100m", domain="EUROPE", store="user")

    results

.. parsed-literal::

───────────────────────────────────────────────────────────────────────────────────────────
  data_type                 footprints
  site                      tac
  domain                    europe
  model                     name
  inlet                     100m
  height                    100m
  species                   inert
  met_model                 not_set
  start_date                2016-07-01 00:00:00+00:00
  end_date                  2016-07-31 23:59:59+00:00
  time_period               1 hour
  max_longitude             39.38
  min_longitude             -97.9
  max_latitude              79.057
  min_latitude              10.729
  high_time_resolution      false
  high_spatial_resolution   false
  short_lifetime            false
  heights                   [500.0, 1500.0, 2500.0, 3500.0, 4500.0, 5500.0, 6500.0, 7500.0,
                            8500.0, 9500.0, 10500.0, 11500.0, 12500.0, 13500.0, 14500.0,
                            15500.0, 16500.0, 17500.0, 18500.0, 19500.0]
  variables                 ['fp', 'air_temperature', 'air_pressure', 'wind_speed',
                            'wind_from_direction', 'atmosphere_boundary_layer_thickness',
                            'release_lon', 'release_lat', 'particle_locations_n',
                            'particle_locations_e', 'particle_locations_s',
                            'particle_locations_w', 'mean_age_particles_n',
                            'mean_age_particles_e', 'mean_age_particles_s',
                            'mean_age_particles_w']
  uuid                      582cf61e-0dcf-46ce-b048-771474c738a4
  original_file_hashes      {'v1': {'7a860dbc929b7c9ea553cb3d480e53bfe5a2af3b':
                            'TAC-100magl_UKV_EUROPE_201607.nc'}}
  latest_version            v1
  timestamp                 2024-04-16 09:04:23.842415+00:00
  versions                  {'v1': ['2016-07-01-00:00:00+00:00_2016-07-31-23:59:59+00:00']}
  object_store              /home/gareth/openghg_store
 ───────────────────────────────────────────────────────────────────────────────────────────

Towards the bottom of this ``SearchResults`` printout is the original file data, stored in the
``original_file_hashes`` metadata key. Each version of the data in the Datasource has each of files used
when adding the data to the Datasource in the object store. The data is stored as key value pairs of the
SHA1 hash of the file and the filename. Here we only have one version of data so let's take the
hashes for that version

.. code-block:: ipython3

    metadata = results.metadata["582cf61e-0dcf-46ce-b048-771474c738a4"]
    hash_data = metadata["original_file_hashes"]["v1"]
    hash_data

    {'7a860dbc929b7c9ea553cb3d480e53bfe5a2af3b': 'TAC-100magl_UKV_EUROPE_201607.nc'}

Export original file
--------------------

How we have the hash data we can retrieve the files from the object store. To export the files
we'll need a folder to export them to. Let's create a folder in our home directory for this.

.. code-block:: ipython3

    from pathlib import Path

    export_folder = Path.home() / "exported_footprints"
    export_folder.mkdir()

With this folder create we'll use ``retrieve_original_files`` to export the compressed files from the
object store. Behind the scenes OpenGHG will perform a lookup using the filename and hash and
then decompress the file into the folder given.

.. code-block:: ipython3

    from openghg.retrieve import retrieve_original_files

    ?retrieve_original_files

    Signature:
    retrieve_original_files(
        store: str,
        data_type: str,
        hash_data: Dict,
        output_folderpath: Union[str, pathlib.Path],
    ) -> None
    Docstring:
    Retrieve the original files used when standardising data. The hash_data argument
    should be the {file_hash: filename, ...} format as stored for each version of data
    in the object store.

    Args:
        store: Object store to retrieve from
        data_type: Data type, e.g. footprints, surface etc
        hash_data: Hash data dictionary from metadata
        output_folderpath: The folder to save the retrieved files to
    Returns:
        None
    File:      ~/home/gareth/Devel/openghg/openghg/retrieve/_original.py
    Type:      function


.. code-block:: ipython3

    retrieve_original_files(store="user", data_type="footprints", hash_data=hash_data, output_folderpath=export_folder)

We now expect there to be the original file in the export folder

.. code-block:: ipython3

    list(export_folder.iterdir())

    [PosixPath('/home/gareth/exported_footprints/TAC-100magl_UKV_EUROPE_201607.nc')]

With the ability to retrieve the original files from the object store we can perform the
standardisation process again is we realise there was missing metadata or issues with the process.
