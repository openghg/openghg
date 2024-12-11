Creating an ObsPack
===================

In this tutorial we're going to demonstrate how you can create an obspack
from the observation data stored within an object store.

*At the moment this includes the data_types "surface" and "column" only
with plans to expand this to all data types going forward.*

Using the tutorial object store
-------------------------------

As in the :ref:`initial tutorial <using-the-tutorial-object-store>`,
we will use the tutorial object store to avoid cluttering your personal
object store.

.. code:: ipython3

    from openghg.tutorial import use_tutorial_store

    use_tutorial_store()

Now we'll add some data to the tutorial store.

.. code:: ipython3

    from openghg.tutorial import populate_surface_data

    populate_surface_data()


1. Compiling the data
---------------------

At the moment the data is compiled from using search terms to find the data in the
object store. This can either be suplied using an input search file or a DataFrame
containing the search terms.

An example of an input search file would be:

|   site,inlet,species,obs_type
|   tac,185m,co2,surface-insitu
|   tac,50m-100m,ch4,surface-insitu
|   bsd,42m,ch4,surface-flask

where the column headings are the names of keys in the object store and an additional
``obs_type`` key is included in the header to describe the expected data within the obspack.

As with the ``get_obs_surface()`` function, ranges of inlets can be specified. This is shown
in the third row of the file above to include the 50m-100m inlets for the Tacolneston site.
This will create a combined netcdf file containing multiple inlets
with the inlet set to "multiple" in the file naming convention.

Details of the accepted observation types can be found by using the `define_obs_types()` function:

.. code:: ipython3

    from openghg.datapack import define_obs_types
    define_obs_types()

If no ``obs_type`` column is specified, it will be assumed that all data is the "surface-insitu" type.

An input search DataFrame is very similar to this format, where the column names
within the DataFrame are the key names to use when searching with the addition of the `obs_type` column.

.. code:: ipython3

    import pandas as pd

    search_df = pd.DataFrame({"site": ["tac", "tac", "bsd"],
                              "inlet": ["185m", "50m-100m", "42m"],
                              "species": ["co2", "ch4", "ch4"],
                              "obs_type": ["surface-insitu", "surface-insitu", "surface-flask"],
                              }
    )

    search_df

*Note: this currently expects one entry to be found per search row. If multiple
sets of data are found for a given search this will raise an error.*


2. Creating an obspack
----------------------

To create the obspack from your search details, the ``create_obspack()`` function can be
called. For this we need to supply the output_directory and a name for the obspack.

For the tutorial we'll create a temporary directory to store the output within but
this will default to the user's home directory otherwise:

.. code:: ipython3

    from tempfile import TemporaryDirectory
    output_folder = TemporaryDirectory().name

.. code:: ipython3

    import pathlib
    from openghg.datapack import create_obspack  

.. code:: ipython3

    obspack_name = "test_obspack_v1"

    create_obspack(search_df=search_df,
                   output_folder=output_folder,
                   obspack_name=obspack_name)

Running this will create an new folder within the temporary folder (defined by ``output_folder``)
called "test_obspack_v1".

For this obspack the expected structure is:

    test_obspack_v1/

        obspack_README.md

        site_index_details*.txt

        site_insitu/

            ch4_bsd_multiple_surface-insitu_v1.nc

            co2_tac_185m_surface-insitu_v1.nc

        site-flask/

            ch4_bsd_42m_surface-flask_v1.nc

This includes a few different elements

1. The obspack_README.md is a release file which contains an overall fair use statement for an obspack.
This is included by default unless alternative release files are specified.
2. The site_index_details\*.txt contains a summary of the site details for the data
included within the file. This includes the data owner details so they can be contacted
when using the data.
3. The data itself is contained within sub-folders split by observation types. The naming
convention is currently based on the ``species``, ``site``, ``inlet`` and ``obs_type`` but we aim to
expand this to include other search terms as appropriate.

This can be investigated within a notebook by running the ``ls`` command to show the local folder structure
and files. As this is a terminal command this must be pre-fixed by the ``%`` character.

::

    %ls "{output_folder}/test_obspack_v1"

::

    %ls "{output_folder}/test_obspack_v1/surface-insitu"

::

    %ls "{output_folder}/test_obspack_v1/surface-flask"

*Version included within the site filenames is currently just the version for the obspack but should be updated
to be the internal version from the object store.*

3. Obspack versions
-------------------

To allow different versions of the obspack to be created automatically, instead of an ``obspack_name``
an ``obspack_stub`` can be supplied instead. Based on other detected or supplied obspack names
this will choose a new version. Both minor and major versions can be created depending on the inputs
supplied.

.. code:: ipython3

    obspack_stub = "test_obspack"

    create_obspack(search_df=search_df,
                   output_folder=output_folder,
                   obspack_name=obspack_name)

This will create a new obspack with a version based on other folder which have the same
``obspack_stub`` found within the same folder. If the previous obspack has been created as above,
this new obspack will be called "test_obspack_v2".

To create a new minor version, this can be run with the ``minor_version_only`` flag:

.. code:: ipython3

    obspack_stub = "test_obspack"

    create_obspack(search_df=search_df,
                   output_folder=output_folder,
                   obspack_name=obspack_name,
                   minor_version_only=True)

If the previous set of obspacks have been created, this new obspack will be called "test_obspack_v2.1".

To create a new major version,  this can be run with the ``major_version_only`` flag:

.. code:: ipython3

    obspack_stub = "test_obspack"

    create_obspack(search_df=search_df,
                   output_folder=output_folder,
                   obspack_name=obspack_name,
                   major_version_only=True)

If the previous set of obspacks have been created, this new obspack will be called "test_obspack_v3".

4. Release files
----------------

The obspack_README.md will be included in every obspack by default but alternative files
can be passed instead for inclusion using the ``release_files`` input to ``create_obspack()`` function.

5. Cleanup
----------

If you're finished with the data in this tutorial you can cleanup the
tutorial object store using the ``clear_tutorial_store`` function.

.. code:: ipython3

    from openghg.tutorial import clear_tutorial_store

.. code:: ipython3

    clear_tutorial_store()
